"""
Worker Task Monitor monitors the count of tasks currently getting executed by the worker
on which the monitor is running.
"""

# Python imports
import json
import logging
import os
import signal
from datetime import datetime, timedelta
from enum import Enum
from multiprocessing import shared_memory
from builtins import memoryview
import math
import time
from typing import Any, Dict, List

# 3rd-party imports
from dateutil.tz import tz
from mypy_boto3_sqs.client import SQSClient
import boto3
import botocore
import psutil

# Our imports
from mwaa.utils.statsd import get_statsd

EOF_TOKEN = "EOF_TOKEN"

# The SQS channel needs to maintain data regarding the SQS messages that it is currently
# consuming. This data can be used by the MWAA worker task monitor to check if a worker
# is idle or not. The data is stored in the shared memory blocks defined below.  The
# shared memory blocks will have a definite size which is calculated here.
#
# This per tasks buffer size allows us to store data for each incoming SQS message like
# the airflow task command contained inside and the SQS message receipt handle. The
# airflow task command helps with correlating an SQS message with its corresponding
# Airflow task process and the receipt handle helps with sending the SQS message back to
# the queue if needed.
#
# Furthermore, if celery fails to remove the message from the queue, the message data
# will still be present in the shared memory blocks defined below. This limit will
# provide the needed flexibility to go beyond what is actually needed for the happy case
# scenario and allow the cleanup process to remove the data from the shared memory
# blocks without running out of space.
BUFFER_SIZE_PER_TASK = 2500
CELERY_WORKER_TASK_LIMIT = int(
    os.environ.get("AIRFLOW__CELERY__WORKER_AUTOSCALE", "80,80").split(",")[0]
)
CELERY_TASKS_BUFFER_SIZE = CELERY_WORKER_TASK_LIMIT * BUFFER_SIZE_PER_TASK

# The command for the process executing an Airflow task has this prefix.
AIRFLOW_TASK_PROCESS_COMMAND_PREFIX = "airflow tasks run"
TRANSPORT_OPTIONS_ENV_KEY = (
    "AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__PREDEFINED_QUEUES"
)
DEFAULT_QUEUE_ENV_KEY = "AIRFLOW__CELERY__DEFAULT_QUEUE"

# A worker maybe be busy loading the required libraries or polling messages from the environment SQS queue, so we allow the worker to
# warm up before checking for idleness or cleaning abandoned resources.
IDLENESS_CHECK_WARMUP_WAIT_PERIOD = timedelta(minutes=3)
CLEANUP_ABANDONED_RESOURCES_WARMUP_WAIT_PERIOD = timedelta(minutes=3)
# If the idleness check is made too aggressively, then we will be reusing the result of the previous check till
# the check delay threshold is reached.
IDLENESS_CHECK_DELAY_PERIOD = timedelta(seconds=1)
# The worker should be idle for some consecutive checks before being declared idle.
CONSECUTIVE_IDLENESS_CHECK_THRESHOLD = 2
# The monitor is also responsible for performing cleanups/corrections of in-memory state in case of issues such as inability to
# terminate the Airflow task process once it has finished or deleting the message from the environment SQS queue. This process requires
# scanning of multiple shared memory blocks and possibly executing SQS operations, so we do this only after a time threshold since
# the last cleanup is breached.
CLEANUP_ABANDONED_RESOURCES_DELAY_PERIOD = timedelta(minutes=1)
# If in the improbable case of the worker picking up new tasks after having paused its consumption, we reset the worker to non-idle
# state and backoff from checking for idleness for a certain threshold.
IDLENESS_RESET_BACKOFF_PERIOD = timedelta(minutes=1)
# In case of issues, signals can arrive late and out of order. So, we scan all unprocessed signals within a time range.
# Only a handful of signals are expected for each worker, so this repeated processing should be very light.
SIGNAL_SEARCH_TIME_RANGE = timedelta(hours=1)
# If a worker activation signal has not been received in a certain threshold, then we give up on waiting anymore for the signal
# and exit the worker. The assumption here is that the signal is somehow lost and will not arrive at all. So, exiting this non-active
# worker will allow the worker to be replaced by a new one.
ACTIVATION_WAIT_TIME_LIMIT = timedelta(minutes=10)
# Worker will be allowed a specific time range for a graceful shutdown starting from the moment of processing a termination signal
# before they are forcibly killed.
TERMINATION_TIME_LIMIT = timedelta(hours=12)

BOTO_RETRY_CONFIGURATION = botocore.config.Config(  # type: ignore
    retries={
        # The standard retry mode provides exponential backoff with a base of 2 and max
        # backoff of 20 seconds. So, with 4 retries, we will try for 2 + 4 + 8 + 16 + 20
        # = 50 seconds before giving up.
        "max_attempts": 5,
        "mode": "standard",
    }
)
MWAA_SIGNALS_DIRECTORY = "/usr/local/mwaa/signals"

CeleryTask = Dict[str, Any]


logger = logging.getLogger(__name__)


class CeleryStateUpdateAction(Enum):
    """
    A simple enum to define the type of operations that can be carried out when updating
    the celery state (memory block containing the current in-flight tasks related data).
    """

    # Add data specific to a single Airflow task to the celery state.
    ADD = 1
    # Remove data specific to a single Airflow task from the celery state.
    REMOVE = 2


def _get_padded_bytes_from_str(raw_data: str):
    data = raw_data + EOF_TOKEN
    data_bytes = bytes(data, "utf-8")
    data_bytes += b"0" * (CELERY_TASKS_BUFFER_SIZE - len(data_bytes))
    return data_bytes


def _get_str_from_padded_bytes(raw_data: memoryview):
    data = str(raw_data, "utf-8")
    return data[: data.index(EOF_TOKEN)]


def _get_celery_tasks(celery_state: shared_memory.SharedMemory) -> List[CeleryTask]:
    return json.loads(
        _get_str_from_padded_bytes(celery_state.buf[:CELERY_TASKS_BUFFER_SIZE])
    )


# Create a shared memory block which the Worker Task Monitor and the Celery SQS Channel
# will use to share the internal state of current work load across the two processes. It
# is maintained by the SQS channel and has information about the current in-flight
# tasks.
def _create_shared_mem_celery_state():
    celery_state_block_name = f'celery_state_{os.environ.get("AIRFLOW_ENV_ID", "")}'
    celery_state = shared_memory.SharedMemory(
        create=True, size=CELERY_TASKS_BUFFER_SIZE, name=celery_state_block_name
    )
    initial_worker_tasks_state = "[]"
    celery_state.buf[:CELERY_TASKS_BUFFER_SIZE] = _get_padded_bytes_from_str(
        initial_worker_tasks_state
    )
    return celery_state


# Create a shared memory block which the Worker Task Monitor and the Celery SQS Channel
# will use to signal the toggle of a flag which tells the Celery SQS channel to
# pause/unpause further consumption of available SQS messages. It is maintained by the
# worker monitor.
# When MWAA signal handling is enabled, the Airflow Task consumption will be turned off by default and
# it will be enabled only when the activation signal has been received by the worker.
def _create_shared_mem_work_consumption_block(mwaa_signal_handling_enabled: bool):
    celery_work_consumption_block_name = (
        f'celery_work_consumption_{os.environ.get("AIRFLOW_ENV_ID", "")}'
    )
    celery_work_consumption_flag_block = shared_memory.SharedMemory(
        create=True, size=1, name=celery_work_consumption_block_name
    )
    celery_work_consumption_flag_block.buf[0] = 1 if mwaa_signal_handling_enabled else 0
    return celery_work_consumption_flag_block


# Create a shared memory block which the Worker Task Monitor and the Celery SQS Channel
# will use to share the internal state of current work load across the two processes. It
# is maintained by the Worker Task Monitor and has information about the current
# in-flight tasks which needs to be cleaned up from 'celery_state'. The second blob is
# used because worker task monitor cannot write into 'celery_state'. If worker task
# monitor was to directly update the 'celery_state', then chances are that changes
# happening concurrently at the worker task monitor and the SQS channel, can cause
# changes to be overwritten by one another.
def _create_shared_mem_cleanup_celery_state():
    cleanup_celery_state_block_name = (
        f'cleanup_celery_state_{os.environ.get("AIRFLOW_ENV_ID", "")}'
    )
    cleanup_celery_state = shared_memory.SharedMemory(
        create=True, size=CELERY_TASKS_BUFFER_SIZE, name=cleanup_celery_state_block_name
    )
    initial_worker_tasks_state = "[]"
    cleanup_celery_state.buf[:CELERY_TASKS_BUFFER_SIZE] = _get_padded_bytes_from_str(
        initial_worker_tasks_state
    )
    return cleanup_celery_state


def _update_celery_state(
    celery_state: shared_memory.SharedMemory,
    celery_task: CeleryTask,
    update_action: CeleryStateUpdateAction,
):
    current_celery_tasks = _get_celery_tasks(celery_state)
    task_index = _get_celery_task_index(celery_task, current_celery_tasks)
    if update_action == CeleryStateUpdateAction.ADD and task_index == -1:
        current_celery_tasks.append(celery_task)
    elif update_action == CeleryStateUpdateAction.REMOVE and task_index != -1:
        current_celery_tasks.pop(task_index)
    celery_state.buf[:CELERY_TASKS_BUFFER_SIZE] = _get_padded_bytes_from_str(
        json.dumps(current_celery_tasks)
    )


def _get_airflow_process_id_mapping():
    """
    Get the list of all processes using psutil and then create a mapping of process
    command to parent process ID and process ID.

    :return: Mapping of process command to parent process ID and process ID.
    """
    process_id_map: Dict[str, int] = {}
    for proc in psutil.process_iter(["pid", "cmdline"]):
        if proc.info["cmdline"]:
            command_line = " ".join(proc.info["cmdline"]).strip()
            if AIRFLOW_TASK_PROCESS_COMMAND_PREFIX in command_line:
                command_line = command_line[
                    command_line.index(AIRFLOW_TASK_PROCESS_COMMAND_PREFIX) :
                ]
                process_id_map[command_line] = proc.info["pid"]
    return process_id_map


def _get_celery_task_index(celery_task: CeleryTask, celery_tasks: List[CeleryTask]):
    """
    Get the index of the celery task in the provided list of tasks.

    :param celery_task: Celery task to be searched.
    :param celery_tasks: List of celery tasks to search in.
    :return: Index of the celery task in the provided list of tasks. -1 if no such task exists.
    """
    for index, task in enumerate(celery_tasks):
        if (
            task["command"] == celery_task["command"]
            and task["receipt_handle"] == celery_task["receipt_handle"]
        ):
            return index
    return -1


def _get_celery_command_index(celery_command: str, celery_tasks: List[CeleryTask]):
    """
    Get the index of the celery task matching the provided command.

    :param celery_command: Celery command to be searched.
    :param celery_tasks: List of celery tasks to search in.
    :return: Index of the celery task matching the provided command. -1 if no such task
    exists.
    """
    for index, task in enumerate(celery_tasks):
        if task["command"] == celery_command:
            return index
    return -1


def _cleanup_undead_process(process_id: int):
    """
    Cleanup the undead process.

    :param process_id: The ID of the process.

    :returns A tuple containing the number of process graceful successes, forceful
    successes, and failures, respectively.
    """
    logger.info(f"Cleaning up undead process with ID: {process_id}")

    # For calculating behavioral metrics.
    clean_undead_process_graceful_success = 0
    clean_undead_process_forceful_success = 0
    clean_undead_process_forceful_failure = 0

    try:
        os.kill(process_id, signal.SIGTERM)
        clean_undead_process_graceful_success += 1
    except OSError as sigterm_error:
        logger.info(f"Failed to SIGTERM process {process_id}. Error: {sigterm_error}")

    try:
        os.kill(process_id, signal.SIGKILL)
        clean_undead_process_forceful_success += 1
    except OSError as sigkill_error:
        logger.info(f"Failed to SIGKILL process {process_id}. Error: {sigkill_error}")
        clean_undead_process_forceful_failure += 1

    # Return metrics.
    return (
        clean_undead_process_graceful_success,
        clean_undead_process_forceful_success,
        clean_undead_process_forceful_failure,
    )


def _get_next_unprocessed_signal() -> (str, dict):
    signal_search_start_timestamp = math.ceil((datetime.now(tz=tz.tzutc()) - SIGNAL_SEARCH_TIME_RANGE).timestamp())
    signal_filenames = os.listdir(MWAA_SIGNALS_DIRECTORY) if os.path.exists(MWAA_SIGNALS_DIRECTORY) else []
    sorted_filenames = sorted(signal_filenames)
    for signal_filename in sorted_filenames:
        # In case of issues, signals can arrive late and out of order. So, we scan all unprocessed signals in a search time range.
        # Only a handful of signals are expected for each worker, so this repeated processing should be very light.
        signal_file_path = os.path.join(MWAA_SIGNALS_DIRECTORY, signal_filename)
        file_timestamp = os.path.getctime(signal_file_path)
        if file_timestamp > signal_search_start_timestamp:
            with open(signal_file_path, "r") as file_data:
                try:
                    signal_data = json.load(file_data)
                except json.JSONDecodeError as e:
                    logger.info(f"Error decoding file {signal_file_path}, signal will be ignored: {e}")
                    signal_data = None
                if signal_data and not signal_data["processed"]:
                    return signal_file_path, signal_data
    return None, None


def _marked_signal_as_processed(signal_filepath, signal_data):
    signal_data["processed"] = True
    with open(signal_filepath, "w") as file_pointer:
        json.dump(signal_data, file_pointer)
    logger.info(f"Successfully processed signal {signal_data['executionId']}")


class WorkerTaskMonitor:
    """
    Monitor for the task count associated with the worker.

    :param mwaa_signal_handling_enabled: Whether the monitor should expect certain signals to be sent from MWAA.
           These signals will represent MWAA service side events such as start of an environment update.
    """

    def __init__(
        self,
        mwaa_signal_handling_enabled: bool,
    ):
        """
        Initialize a WorkerTaskMonitor instance.
        """
        self.mwaa_signal_handling_enabled = mwaa_signal_handling_enabled

        # A worker maybe be busy loading the required libraries or polling messages from the environment SQS queue,
        # so we allow the worker to warm up before checking for idleness or cleaning abandoned resources.
        self.idleness_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_CHECK_WARMUP_WAIT_PERIOD
        )
        self.cleanup_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + CLEANUP_ABANDONED_RESOURCES_WARMUP_WAIT_PERIOD
        )

        # If the idleness check is made too aggressively, then we will be reusing the result of the previous check till
        # the check delay threshold is reached.
        self.idleness_check_delay_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_CHECK_DELAY_PERIOD
        )
        self.last_idleness_check_result = False
        # The consecutive_idleness_count needs to go over
        # CONSECUTIVE_IDLENESS_CHECK_THRESHOLD to declare the worker as idle.
        self.consecutive_idleness_count = 0

        # If MWAA Signal handling is enabled, then monitor will wait for activation signal before starting consumption of work.
        # Activation signal will be sent when service side changes have been made to ensure that it is safe for worker to start working.
        self.waiting_for_activation = True if self.mwaa_signal_handling_enabled else False
        # The monitor keeps track of the start of the period for which it has been waiting for activation. This is used to check
        # if a time limit has expired and if the monitor should give up.
        self.activation_wait_start = datetime.now(tz=tz.tzutc())
        # If MWAA Signal handling is enabled, then monitor will periodically check if a kill signal has been sent by MWAA for the worker.
        # If the signal is found, the monitor will kill the worker without waiting for the current Airflow tasks to be completed.
        self.marked_for_kill = False
        # If MWAA Signal handling is enabled, then monitor will periodically check if a termination signal has been sent by MWAA for the
        # worker. If the signal is found, the monitor will terminate the worker after waiting for the current Airflow tasks to be completed.
        self.marked_for_termination = False

        # If resume and termination signals are received out of order, then processing them out of order can lead to undesired results.
        # So, we will maintain timestamp of last processed termination or resume signal creation time to check if the latest observed
        # signal should be processed or not.
        self.last_termination_or_resume_signal_timestamp = None
        # When a termination signal is received by a worker, then it is provided TERMINATION_TIME_LIMIT amount of time to graceful
        # shutdown by finishing up the current Airflow tasks. But if termination signals are received late due to an issue, then
        # we need to allow the TERMINATION_TIME_LIMIT to start from the point in time of processing the signal and not the time when the
        # signal was sent from MWAA.
        self.last_termination_processing_time = None

        self.celery_state = _create_shared_mem_celery_state()
        self.celery_work_consumption_block = _create_shared_mem_work_consumption_block(self.mwaa_signal_handling_enabled)
        self.cleanup_celery_state = _create_shared_mem_cleanup_celery_state()
        self.abandoned_celery_tasks_from_last_check: List[CeleryTask] = []
        self.undead_process_ids_from_last_check = []

        self.stats = get_statsd()

        self.closed = False

    def is_worker_idle(self):
        """
        Checks if the worker has gone idle or not. For this to happen, a worker will
        need to have 0 tasks assigned to it for 15 consecutive idleness checks.

        :return: True if the worker has gone idle. False otherwise.
        """

        if self.closed:
            logger.warning(
                "Using is_worker_idle() of a task monitor after it has been closed."
            )
            # Since the worker task monitor has been closed, we are going to assume
            # the worker is idle.
            return True

        # If the warmup timestamp has not expired yet, then we treat worker as busy.
        if datetime.now(tz=tz.tzutc()) < self.idleness_check_warmup_timestamp:
            return False

        # If the delay timestamp has not expired yet, then we do nothing.
        if datetime.now(tz=tz.tzutc()) < self.idleness_check_delay_timestamp:
            return self.last_idleness_check_result
        self.idleness_check_delay_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_CHECK_DELAY_PERIOD
        )

        idleness_check_result = self._get_current_task_count() == 0
        self.consecutive_idleness_count = (
            self.consecutive_idleness_count + 1 if idleness_check_result else 0
        )
        self.last_idleness_check_result = (
            self.consecutive_idleness_count >= CONSECUTIVE_IDLENESS_CHECK_THRESHOLD
        )
        return self.last_idleness_check_result

    def is_marked_for_kill(self):
        """
        Checks if the worker has been marked for kill or not. If MWAA Signal handling is enabled, then monitor will periodically check
        if a kill signal has been sent by MWAA for the worker. If the signal is found, the monitor will kill the worker without waiting
        for the current Airflow tasks to be completed.

        :return: True if the worker has been marked for kill. False otherwise.
        """
        return self.marked_for_kill

    def is_marked_for_termination(self):
        """
        Checks if the worker has been marked for termination or not. If MWAA Signal handling is enabled, then monitor will periodically
        check if a termination signal has been sent by MWAA for the worker. If the signal is found, the monitor will terminate the worker
        after waiting for the current Airflow tasks to be completed.

        :return: True if the worker has been marked for termination. False otherwise.
        """
        return self.marked_for_termination

    def _get_current_task_count(self):
        """
        Get count of tasks currently getting executed on the worker. Any task present in
        both celery_state and cleanup_celery_state is considered as not running on the
        worker.

        :return: Number of current tasks.
        """
        current_celery_tasks = _get_celery_tasks(self.celery_state)
        current_cleanup_celery_tasks = _get_celery_tasks(self.cleanup_celery_state)
        current_task_count = 0
        for task in current_celery_tasks:
            if _get_celery_task_index(task, current_cleanup_celery_tasks) == -1:
                current_task_count += 1
        return current_task_count

    def process_next_signal(self):
        """
        This method is used to process any signals sent by MWAA. This method processes the first signal it finds
        in the chronological order of the available unprocessed signals.
        """
        if not self.mwaa_signal_handling_enabled:
            logger.info("Signal handling is not enabled for this worker.")
            return
        if self.closed:
            logger.warning(
                "Using process_next_signal() of a task monitor "
                "after it has been closed."
            )
            return
        signal_filepath, signal_data = _get_next_unprocessed_signal()
        if not signal_data:
            logger.info("No new signal found.")
            return
        signal_id = signal_data["executionId"]
        signal_type = signal_data["signalType"]
        signal_timestamp = signal_data["createdAt"]
        logger.info(f"Processing signal {signal_id} of type {signal_type} created at {signal_timestamp}")
        if signal_type == "activation":
            self.waiting_for_activation = False
        elif signal_type == "kill":
            self.marked_for_kill = True
        elif signal_type == "termination":
            if (self.last_termination_or_resume_signal_timestamp is None or
                    self.last_termination_or_resume_signal_timestamp < signal_timestamp):
                self.marked_for_termination = True
                self.last_termination_or_resume_signal_timestamp = signal_timestamp
                self.last_termination_processing_time = datetime.now(tz=tz.tzutc())
        elif signal_type == "resume":
            if (self.last_termination_or_resume_signal_timestamp is None or
                    self.last_termination_or_resume_signal_timestamp < signal_timestamp):
                self.marked_for_termination = False
                self.last_termination_or_resume_signal_timestamp = signal_timestamp
                self.last_termination_processing_time = None
        else:
            logger.warning(f"Unknown signal type {signal_type}, ignoring.")
        should_consume_work = not (self.waiting_for_activation or self.marked_for_kill or self.marked_for_termination)
        self.resume_task_consumption() if should_consume_work else self.pause_task_consumption()
        _marked_signal_as_processed(signal_filepath, signal_data)

    def is_activation_wait_time_limit_breached(self):
        """
        This method checks if the time limit for waiting for activation has been breached or not.
        :return: True, if the time limit for waiting for activation has been breached.
        """
        return self.waiting_for_activation and datetime.now(tz=tz.tzutc()) > self.activation_wait_start + ACTIVATION_WAIT_TIME_LIMIT

    def is_termination_time_limit_breached(self):
        """
        This method checks if the termination time limit has been breached or not.
        :return: True, if the worker has been marked for termination and the allowed time limit for termination has been breached.
        """
        return (self.marked_for_termination and self.last_termination_processing_time and
                datetime.now(tz=tz.tzutc()) > self.last_termination_processing_time + TERMINATION_TIME_LIMIT)

    def pause_task_consumption(self):
        """
        celery_work_consumption_block represents the toggle switch for accepting any
        more incoming SQS message from the celery queue which will be used during the
        shutdown procedure. Setting it to 1 will block anymore SQS messages from being
        consumed by the worker.
        """
        if self.closed:
            logger.warning(
                "Using pause_task_consumption() of a task monitor "
                "after it has been closed."
            )
            return

        was_consumption_unpaused = self.celery_work_consumption_block.buf[0] == 0
        self.celery_work_consumption_block.buf[0] = 1
        if was_consumption_unpaused:
            # When we toggle the Airflow Task consumption to paused state, we wait a few seconds in order
            # for any in-flight messages in the SQS broker layer to be processed and
            # corresponding Airflow task instance to be created. Once that is done, we can
            # start gracefully shutting down the worker. Without this, the SQS broker may
            # consume messages from the queue, terminate before creating the corresponding
            # Airflow task instance and abandon SQS messages in-flight.
            logger.info("Pausing task consumption.")
            time.sleep(5)

    def resume_task_consumption(self):
        """
        celery_work_consumption_block represents the toggle switch for accepting any
        more incoming SQS message from the celery queue which will be used during the
        shutdown procedure. Setting it to 0 will reset the blockage created via
        pause_task_consumption method.
        """
        if self.closed:
            logger.warning(
                "Using resume_task_consumption() of a task monitor "
                "after it has been closed."
            )
            return
        was_consumption_paused = self.celery_work_consumption_block.buf[0] == 1
        self.celery_work_consumption_block.buf[0] = 0
        if was_consumption_paused:
            # When we toggle the Airflow Task consumption to unpaused state, we wait a few seconds in order
            # for any in-flight messages in the SQS queue to start getting consumed by
            # the broker layer before checking for worker idleness.
            logger.info("Unpausing task consumption.")
            time.sleep(5)

    def reset_monitor_state(self):
        """
        This will be used in case the SQS message consumption on the worker process is
        unpaused. We backoff for 1 min before checking for idleness again.
        """
        if self.closed:
            logger.warning(
                "Using reset_monitor_state() of a task monitor "
                "after it has been closed."
            )
            return
        self.idleness_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_RESET_BACKOFF_PERIOD
        )

    def cleanup_abandoned_resources(self):
        """
        Cleanup any abandoned SQS messages from the Celery SQS Channel and undead/zombie
        task processes.
        """
        if self.closed:
            logger.warning(
                "Using cleanup_abandoned_resources() of a task monitor "
                "after it has been closed."
            )
            return

        # If the warmup timestamp has not expired yet, then we do nothing.
        if datetime.now(tz=tz.tzutc()) < self.cleanup_check_warmup_timestamp:
            return
        self.cleanup_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + CLEANUP_ABANDONED_RESOURCES_DELAY_PERIOD
        )

        process_id_map = _get_airflow_process_id_mapping()
        current_celery_tasks = _get_celery_tasks(self.celery_state)

        self._return_all_abandoned_task_to_queue(current_celery_tasks, process_id_map)
        self._cleanup_all_undead_processes(current_celery_tasks, process_id_map)

    def close(self):
        """
        This will be called when the worker has been terminated. We close these blocks
        in order to avoid receiving a warning message in the logs. We are not calling
        unlink here intentionally because both the task monitor and the Celery SQS
        channel references these blocks and when the worker is shutting down, we do not
        control which process will be killed first. Calling unlink may result in a
        warning message showing up in the customer side logs causing unnecessary
        confusion.
        """
        if self.closed:
            # Already closed.
            return

        logger.info("Closing task monitor...")
        self.pause_task_consumption()

        # Report a metric about the number of current task, and a warning in case this is greater than zero. If the worker was
        # marked for killing or was marked for termination and the allowed time limit for termination has been breached, then we do
        # not report this metric because this task interruption is expected and should not be used for alarming.
        task_count = self._get_current_task_count()
        if task_count > 0:
            logger.warning("There are non-zero ongoing tasks.")
        if self.marked_for_kill or self.is_termination_time_limit_breached():
            if task_count > 0:
                logger.warning("Worker is being forcibly shutdown via expected methods, "
                               "interrupted_tasks_at_shutdown metric will not be emitted.")
        else:
            self.stats.incr(f"mwaa.task_monitor.interrupted_tasks_at_shutdown", task_count)  # type: ignore

        # Close shared memory objects.
        self.celery_state.close()
        self.celery_work_consumption_block.close()
        self.cleanup_celery_state.close()

        self.closed = True

        logger.info("Task monitor closed.")

    def _return_all_abandoned_task_to_queue(
        self,
        current_celery_tasks: List[CeleryTask],
        process_id_map: Dict[str, int],
    ):
        # For calculating behavioral metrics.
        clean_celery_message_error_no_queue = 0
        clean_celery_message_success = 0
        clean_celery_message_error_sqs_op = 0

        # Checking if celery task cleanup from the past run has been completed.
        current_cleanup_celery_tasks = _get_celery_tasks(self.cleanup_celery_state)
        for cleanup_celery_task in current_cleanup_celery_tasks:
            if _get_celery_task_index(cleanup_celery_task, current_celery_tasks) == -1:
                _update_celery_state(
                    self.cleanup_celery_state,
                    cleanup_celery_task,
                    CeleryStateUpdateAction.REMOVE,
                )
                logger.info(f"Cleanup complete for celery task {cleanup_celery_task}.")
        current_cleanup_celery_tasks = _get_celery_tasks(self.cleanup_celery_state)

        # Cleanup abandoned SQS messages from the Celery SQS Channel.
        potentially_abandoned_celery_tasks: List[CeleryTask] = []
        for celery_task in current_celery_tasks:
            if celery_task["command"] not in process_id_map:
                if (
                    _get_celery_task_index(celery_task, current_cleanup_celery_tasks)
                    == -1
                ):
                    if (
                        _get_celery_task_index(
                            celery_task, self.abandoned_celery_tasks_from_last_check
                        )
                        == -1
                    ):
                        potentially_abandoned_celery_tasks.append(celery_task)
                    else:
                        (
                            clean_celery_message_error_no_queue,
                            clean_celery_message_success,
                            clean_celery_message_error_sqs_op,
                        ) = self._return_abandoned_task_to_queue(celery_task)
        self.abandoned_celery_tasks_from_last_check = potentially_abandoned_celery_tasks

        # Report behavioural metrics.
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_celery_message_error_no_queue",
            clean_celery_message_error_no_queue,
        )
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_celery_message_success",
            clean_celery_message_success,
        )
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_celery_message_error_sqs_op",
            clean_celery_message_error_sqs_op,
        )

    def _cleanup_all_undead_processes(
        self,
        current_celery_tasks: List[CeleryTask],
        process_id_map: Dict[str, int],
    ):
        # For calculating behvaioural metrics.
        clean_undead_process_graceful_success = 0
        clean_undead_process_forceful_success = 0
        clean_undead_process_forceful_failure = 0

        # Cleanup any undead task processes.
        potentially_undead_process_ids: List[int] = []
        for task_command in process_id_map:
            if _get_celery_command_index(task_command, current_celery_tasks) == -1:
                if (
                    process_id_map[task_command]
                    not in self.undead_process_ids_from_last_check
                ):
                    potentially_undead_process_ids.append(process_id_map[task_command])
                else:
                    (
                        clean_undead_process_graceful_success,
                        clean_undead_process_forceful_success,
                        clean_undead_process_forceful_failure,
                    ) = _cleanup_undead_process(process_id_map[task_command])
        self.undead_process_ids_from_last_check = potentially_undead_process_ids

        # Report behavioural metrics.
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_undead_process_graceful_success",
            clean_undead_process_graceful_success,
        )
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_undead_process_forceful_success",
            clean_undead_process_forceful_success,
        )
        self.stats.incr(  # type: ignore
            f"mwaa.task_monitor.clean_undead_process_forceful_failure",
            clean_undead_process_forceful_failure,
        )

    def _return_abandoned_task_to_queue(self, celery_task: CeleryTask):
        """
        Cleanup the abandoned SQS message from the Celery SQS Channel.
        :param celery_task: Celery task (celery command + SQS receipt handle).
        """
        # For calculating behvaioural metrics.
        clean_celery_message_error_no_queue = 0
        clean_celery_message_success = 0
        clean_celery_message_error_sqs_op = 0

        logger.info(
            "Cleaning up abandoned SQS message corresponding to task "
            f"state: {celery_task}"
        )
        default_queue_name = os.environ.get(DEFAULT_QUEUE_ENV_KEY)
        celery_queue_details = json.loads(
            os.environ.get(TRANSPORT_OPTIONS_ENV_KEY, "{}")
        ).get(default_queue_name)
        celery_queue_url = (
            celery_queue_details.get("url") if celery_queue_details else None
        )
        if not celery_queue_url:
            logger.info(
                f"Unable to cleanup abandoned SQS message for task state {celery_task}."
                " No default queue found."
            )
            clean_celery_message_error_no_queue += 1

        else:
            sqs: SQSClient = boto3.client(  # type: ignore
                "sqs",
                region_name=os.environ["AWS_REGION"],
                config=BOTO_RETRY_CONFIGURATION,  # type: ignore
            )
            try:
                sqs.change_message_visibility(
                    QueueUrl=celery_queue_url,
                    ReceiptHandle=celery_task["receipt_handle"],
                    VisibilityTimeout=0,
                )
                clean_celery_message_success += 1
            except botocore.exceptions.ClientError as error:  # type: ignore
                logger.info(
                    f"Unable to cleanup abandoned SQS message for task state {celery_task}."
                    f" Error: {error}"
                )
                clean_celery_message_error_sqs_op += 1
            _update_celery_state(
                self.cleanup_celery_state, celery_task, CeleryStateUpdateAction.ADD
            )

        # For calculating behvaioural metrics.
        return (
            clean_celery_message_error_no_queue,
            clean_celery_message_success,
            clean_celery_message_error_sqs_op,
        )
