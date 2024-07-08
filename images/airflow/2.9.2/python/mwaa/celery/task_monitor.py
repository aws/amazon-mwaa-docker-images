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
    os.environ.get("AIRFLOW__CELERY__WORKER_AUTOSCALE", "20,20").split(",")[0]
)
CELERY_TASKS_BUFFER_SIZE = CELERY_WORKER_TASK_LIMIT * BUFFER_SIZE_PER_TASK

# The command for the process executing an Airflow task has this prefix.
AIRFLOW_TASK_PROCESS_COMMAND_PREFIX = "airflow tasks run"
TRANSPORT_OPTIONS_ENV_KEY = (
    "AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__PREDEFINED_QUEUES"
)
DEFAULT_QUEUE_ENV_KEY = "AIRFLOW__CELERY__DEFAULT_QUEUE"

# Allow at least 3 minutes for the worker to warm up before checking for idleness or
# cleaning abandoned resources.
IDLENESS_CHECK_WARMUP_WAIT_PERIOD = timedelta(minutes=3)
CLEANUP_ABANDONED_RESOURCES_WARMUP_WAIT_PERIOD = timedelta(minutes=3)
# Allow at least 1 second between any two consecutive idleness checks.
IDLENESS_CHECK_DELAY_PERIOD = timedelta(seconds=1)
# The worker should be idle for at least 2 consecutive idleness check before being
# declared idle.
CONSECUTIVE_IDLENESS_CHECK_THRESHOLD = 2
# Allow at least 1 minute between any two consecutive abandoned resource cleanups.
CLEANUP_ABANDONED_RESOURCES_DELAY_PERIOD = timedelta(minutes=1)
# If in the improbable case of the worker picking up new tasks after having paused its
# consumption, we reset the worker to non-idle state and backoff from checking for
# idleness for a minute.
IDLENESS_RESET_BACKOFF_PERIOD = timedelta(minutes=1)
BOTO_RETRY_CONFIGURATION = botocore.config.Config(  # type: ignore
    retries={
        # The standard retry mode provides exponential backoff with a base of 2 and max
        # backoff of 20 seconds. So, with 4 retries, we will try for 2 + 4 + 8 + 16 + 20
        # = 50 seconds before giving up.
        "max_attempts": 5,
        "mode": "standard",
    }
)

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
def _create_shared_mem_work_consumption_block():
    celery_work_consumption_block_name = (
        f'celery_work_consumption_{os.environ.get("AIRFLOW_ENV_ID", "")}'
    )
    celery_work_consumption_flag_block = shared_memory.SharedMemory(
        create=True, size=1, name=celery_work_consumption_block_name
    )
    celery_work_consumption_flag_block.buf[0] = 0
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


class WorkerTaskMonitor:
    """
    Monitor for the task count associated with the worker.
    """

    def __init__(self):
        """
        Initialize a WorkerTaskMonitor instance.
        """
        # Allow at least 3 minutes for the worker to warm up before checking for
        # idleness or cleaning abandoned resources.
        self.idleness_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_CHECK_WARMUP_WAIT_PERIOD
        )
        self.cleanup_check_warmup_timestamp = (
            datetime.now(tz=tz.tzutc()) + CLEANUP_ABANDONED_RESOURCES_WARMUP_WAIT_PERIOD
        )

        # Allow at least 1 second to elapse between any two checks for idleness.
        self.idleness_check_delay_timestamp = (
            datetime.now(tz=tz.tzutc()) + IDLENESS_CHECK_DELAY_PERIOD
        )
        self.last_idleness_check_result = False
        # The consecutive_idleness_count needs to go over
        # CONSECUTIVE_IDLENESS_CHECK_THRESHOLD to declare the worker as idle.
        self.consecutive_idleness_count = 0

        self.celery_state = _create_shared_mem_celery_state()
        self.celery_work_consumption_block = _create_shared_mem_work_consumption_block()
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

        logger.info("Pausing task consumption.")
        self.celery_work_consumption_block.buf[0] = 1

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
        logger.info("Unpausing task consumption.")
        self.celery_work_consumption_block.buf[0] = 0

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

        # Report a metric about the number of current task, and a warning in case this
        # is greater than zero.
        task_count = self._get_current_task_count()
        if task_count > 0:
            logger.warning("There are non-zero ongoing tasks.")
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
