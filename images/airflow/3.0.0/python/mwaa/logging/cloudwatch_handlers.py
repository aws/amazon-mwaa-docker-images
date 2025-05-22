"""
This module contain multiple log handlers to support integration with CloudWatch Logs.

It contains the BaseLogHandler class, which has some common functionality needed by
all of the handlers. It also contains a couple other handlers for different log
categories, e.g. TaskLogHandler for handling task logs, SubprocessLogHandler for
handling scheduler/worker/etc logs, and so on.
"""

# Python imports
import logging
import os
import re
import sys
import traceback

# 3rd party imports
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.log.cloudwatch_task_handler import (
    CloudwatchTaskHandler,
)
from airflow.utils.helpers import parse_template_string
from mypy_boto3_logs.client import CloudWatchLogsClient
from typing import Dict
import boto3
import socket
import time
import watchtower

# Our imports
from mwaa.logging.utils import parse_arn, throttle
from mwaa.utils.statsd import get_statsd


LOG_GROUP_INIT_WAIT_SECONDS = 900
ERROR_REPORTING_WAIT_SECONDS = 60


# fmt: off
# IMPORTANT NOTE: The time complexity of log inspection is O(M*N) where M is the number
# of logs and N is the number of log patterns. As such, each new pattern added will
# increase the time complexity by O(M), which could be substantial adition for noisy
# environments. Care should, thus, be taken to only add the really important patterns.
# Additionally, there is no reason why we shouldn't add those
_PATTERNS = [
    # psycopg2.OperationalError
    (re.compile(r"psycopg2\.OperationalError"), "psycopg2"),
    # Timeout errors
    (re.compile( r"airflow\.exceptions\.AirflowTaskTimeout: DagBag import timeout for .+ after"), "DagImportTimeout"),
    (re.compile(r"airflow\.exceptions\.AirflowTaskTimeout"), "TaskTimeout"),
    # base_executor.py
    (re.compile(r"could not queue task"), "TaskQueueingFailure"),
    # celery_executor.py
    (re.compile(r"Adopted tasks were still pending after"), "AdoptedTaskStillPending"),
    (re.compile(r"Celery command failed on host:"), "CeleryCommandFailure"),
    (re.compile(r"Failed to execute task"), "CeleryTaskExecutionFailure"),
    (re.compile(r"execute_command encountered a CalledProcessError"), "ExecuteCommandCalledProcessError"),
    # dag_processing.py
    (re.compile( r"DagFileProcessorManager \(PID=.+\) last sent a heartbeat .+ seconds ago! Restarting it"), "DagFileProcessorManagerNoHeartbeat"),
    # dagrun.py
    (re.compile(r"Marking run .+ failed"), "DagRunFailure"),
    (re.compile(r"Deadlock; marking run .+ failed"), "DagRunDeadlock"),
    # taskinstance.py
    (re.compile(r"Recording the task instance as FAILED"), "TaskInstanceFailure"),
    # taskinstance.py and local_task_job.py
    (re.compile(r"Received SIGTERM\. Terminating subprocesses."), "SIGTERM"),
    # scheduler_job.py
    (re.compile(r"Couldn\'t find dag .+ in DagBag/DB!"), "DagNotFound"),
    (re.compile(r"Execution date is in future:"), "ExecutionDateInFuture"),
    # standard_task_runner.py
    (re.compile( r"Job .+ was killed before it finished (likely due to running out of memory)"), "JobKilled"),
]
# fmt: on


class BaseLogHandler(logging.Handler):
    """Shared functionality across our internal CloudWatch log handlers."""

    def __init__(self, log_group_arn: str, kms_key_arn: str | None, enabled: bool):
        """
        Initialize the instance.

        Arguments:
            log_group_arn - The ARN of the log group where logs will be published.
            kms_key_arn - The ARN of the KMS key to use when creating the log group
              if necessary.
            enabled - Whether this handler is actually enabled, or just does nothing.
              This makes it easier to control enabling and disabling logging without
              much changes to the logging configuration.
        """
        self.log_group_arn = log_group_arn
        self.kms_key_arn = kms_key_arn
        self.enabled = enabled
        if not self.enabled:
            self._print(
                "CloudWatch logging is disabled for %s" % self.__class__.__name__
            )
        self.log_group_name, self.region_name = parse_arn(log_group_arn)
        self.handler = None
        self.logs_source = "Unknown"

        # TODO Find a nice and unambiguous solution to the craziness of super() and MRO.
        logging.Handler.__init__(self)

        self.stats = get_statsd()

    def create_watchtower_handler(
        self,
        stream_name: str,
        logs_source: str,
        send_interval_seconds: int = 10,
        use_queues: bool = True,
    ):
        """
        Create the underlying Watchtower handler that we use to publish logs.

        Arguments:
            stream_name - The name of the log stream to publish logs under.
            logs_source - A string identifying the source the logs are coming from, e.g.
              "scheduler". This is used when publishing metrics about logging.
            send_interval_seconds - The interval at which to send logs to CloudWatch.
            use_queues - Whether to use batching to publish logs or not. This is usually
              desired for efficiency, but can have certain problems when used with
              multiprocessing. Use with extra care.
        """
        logs_client: CloudWatchLogsClient = boto3.client("logs")  # type: ignore

        if self.enabled:
            self.handler = watchtower.CloudWatchLogHandler(
                log_group_name=self.log_group_name,
                log_stream_name=stream_name,
                boto3_client=logs_client,
                use_queues=use_queues,
                send_interval=send_interval_seconds,
                create_log_group=False,
            )
            if self.formatter:
                self.handler.setFormatter(self.formatter)
        self.logs_source = logs_source

    def close(self):
        """Close the log handler (by closing the underlying log handler)."""
        if self.handler is not None:
            self.handler.close()
            self.handler = None

    @throttle(ERROR_REPORTING_WAIT_SECONDS)
    def _report_logging_error(self, msg: str):
        """
        Report an error related to logging.

        This method is used to report an error related to logging, along with the
        stack information to aid with debugging. This method is throttled to avoid
        logs pollution.

        Arguments:
            msg - The error message to report.
        """
        self._print(f"MWAA logging error: {msg}")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)

    def _print(self, msg: str):
        print(msg)

    def emit(self, record: logging.LogRecord):
        """
        Emit log records.

        Arguments:
            record - The log record to emit.
        """
        if self.handler:
            # This is a potentially noisy warning that we started seeing because we
            # are still not using pattern matching for metrics allow/block-listing.
            # As a temporary work-around, we are dropping these messages at the handler
            # level. We should, however, fix this issue by setting to True the
            # `metrics_use_pattern_match` flag.
            # More context: https://github.com/aws/amazon-mwaa-docker-images/issues/98
            if "The basic metric validator will be deprecated" in record.getMessage():
                return
            try:
                self.handler.emit(record)  # type: ignore
                self.sniff_errors(record)
            except Exception:
                self.stats.incr(f"mwaa.logging.{self.logs_source}.emit_error", 1)
                self._report_logging_error("Failed to emit log record.")

    def sniff_errors(self, record: logging.LogRecord):
        """
        Check the content of the logs for known errors and report them as metrics.

        Privacy Note: The logs here are customer logs and, thus, we cannot store them,
        so we only check them against a predefined list of Airflow errors and report a
        metric.

        :param record: The log record being sniffed.
        """

        if not hasattr(record, "message"):
            return

        for pattern, metric_dim in _PATTERNS:
            if pattern.search(record.message):
                self.stats.incr(f"mwaa.error_log.{self.logs_source}.{metric_dim}")
                break

    def flush(self):
        """Flush remaining log records."""
        if not self.handler:
            return
        try:
            self.handler.flush()
        except Exception:
            self.stats.incr(f"mwaa.logging.{self.logs_source}.flush_error", 1)
            self._report_logging_error("Failed to flush log records.")


class TaskLogHandler(BaseLogHandler, CloudwatchTaskHandler):
    """A log handler used for Airflow task logs."""

    # this option is required to be able to serve triggerer logs
    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        log_group_arn: str,
        kms_key_arn: str | None,
        enabled: bool,
    ):
        """
        Initialize the instance.

        :param log_group_arn - The ARN of the log group where logs will be published.
        :param kms_key_arn - The ARN of the KMS key to use when creating the log group
            if necessary.
        :param enabled - Whether this handler is actually enabled, or just does nothing.
            This makes it easier to control enabling and disabling logging without
            much changes to the logging configuration.
        """
        self.processors = []
        BaseLogHandler.__init__(self, log_group_arn, kms_key_arn, enabled)
        CloudwatchTaskHandler.__init__(
            self,
            log_group_arn=log_group_arn,
            base_log_folder="",  # We only push to CloudWatch Logs.
        )

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None:
        """
        Provide context to the logger.

        This method is called by Airflow to provide the necessary context to configure
        the handler. In this case, Airflow is passing us the task instance the logs
        are for.

        :param ti: The task instance generating the logs.
        :param identifier: Airflow uses this when relaying exceptional messages to task
        logs from a context other than task itself. We ignore this parameter in this
        handler, i.e. those exceptional messages will go to the same log stream.
        """
        # TODO Consider making use of the 'identifier' argument:
        # https://github.com/aws/amazon-mwaa-docker-images/issues/57
        logs_client: CloudWatchLogsClient = boto3.client("logs")  # type: ignore

        if self.enabled:
            # identical to open-source implementation, except create_log_group set to False
            self.handler = watchtower.CloudWatchLogHandler(
                log_group_name=self.log_group_name,
                log_stream_name=self._render_filename(ti, ti.try_number),  # type: ignore
                boto3_client=logs_client,
                use_queues=True,
                create_log_group=False,
            )

            if self.formatter:
                self.handler.setFormatter(self.formatter)
        else:
            self.handler = None

    def _event_to_str(self, event: Dict[str, str]) -> str:
        # When rendering logs in the UI, the open-source implementation prefixes the
        # logs by their timestamp metadata from the Cloudwatch response. Since the
        # default log format already includes a timestamp within the message, this
        # causes duplicate timestamps to appear and result in "invalid date" rendering
        # in the UI
        #
        # Open-source code: https://github.com/apache/airflow/blob/v2-7-stable/airflow/providers/amazon/aws/log/cloudwatch_task_handler.py#L120
        #
        # TODO: explore option to condition this function by the log_format. i.e. if it
        # does not include a timestamp, add it here. Since customers can freely change
        # the log_format, we should work towards accommodating both cases, but the
        # default behavior is more important
        return event["message"]


class DagProcessorManagerLogHandler(BaseLogHandler):
    """
    A log handler for logs generated by Airflow's DAG Processor Manager.

    The DAG Processor Manager is represented by the DagFileProcessorProcess class from
    Airflow ([1]), and shouldn't be confused with the DagFileProcessorProcess class [2]
    which is responsible for processing a single DAG file.

    [1] https://github.com/apache/airflow/blob/2.9.2/airflow/dag_processing/manager.py#L331
    [2] https://github.com/apache/airflow/blob/2.9.2/airflow/dag_processing/processor.py#L69
    """

    def __init__(
        self, log_group_arn: str, kms_key_arn: str, stream_name: str, enabled: bool
    ):
        """
        Initialize the instance.

        Arguments:
            log_group_arn - The ARN of the log group where logs will be published.
            kms_key_arn - The ARN of the KMS key to use when creating the log group
              if necessary.
            stream_name - The name of the stream under which logs will be published.
            enabled - Whether this handler is actually enabled, or just does nothing.
              This makes it easier to control enabling and disabling logging without
              much changes to the logging configuration.

        [1] https://airflow.apache.org/docs/apache-airflow/2.9.2/configurations-ref.html#config-logging-log-processor-filename-template
        """
        super().__init__(log_group_arn, kms_key_arn, enabled)
        self.create_watchtower_handler(stream_name, "DAGProcessorManager")

    def _print(self, msg: str):
        # The DAG processing loggers are not started in the same way that the Web
        # Server, Scheduler, Worker are (which are standalone processes we start and
        # control). Instead, the DAG Processor is started from within Airflow code.  All
        # of the output from that process is captured and fed to logging. So if the
        # logger itself emits logs, it creates a cycle.
        pass


class DagProcessingLogHandler(BaseLogHandler):
    """
    A log handler for logs generated during processing of a certain DAG.

    This shouldn't be confused with the DagProcessorManagerLogHandler class. See the
    documentation on the latter class for more information.
    """

    def __init__(
        self,
        log_group_arn: str,
        kms_key_arn: str | None,
        stream_name_template: str,
        enabled: bool,
    ):
        """
        Initialize the instance.

        Arguments:
            log_group_arn - The ARN of the log group where logs will be published.
            kms_key_arn - The ARN of the KMS key to use when creating the log group
              if necessary.
            stream_name_template - The template to use for generating the stream name.
              Currently, in the config.py file, we pass the
              "[logging] LOG_PROCESSOR_FILENAME_TEMPLATE" Airflow configuration [1].
            enabled - Whether this handler is actually enabled, or just does nothing.
              This makes it easier to control enabling and disabling logging without
              much changes to the logging configuration.

        [1] https://airflow.apache.org/docs/apache-airflow/2.9.2/configurations-ref.html#config-logging-log-processor-filename-template
        """
        super().__init__(log_group_arn, kms_key_arn, enabled)

        self.stream_name_template, self.filename_jinja_template = parse_template_string(
            stream_name_template
        )

    def set_context(self, filename: str):
        """
        Provide context to the logger.

        This method is called by Airflow to provide the necessary context to configure
        the handler. In this case, Airflow is passing us the name of the DAG file being
        processed.

        :param filename: The name of the DAG file being processed.
        """
        stream_name = self._render_filename(filename)
        self.create_watchtower_handler(
            stream_name,
            logs_source="DAGProcessing",
            # cannot use queues/batching with DAG processing, since the DAG processor
            # process gets terminated without properly calling the flush() method on the
            # handler (most probably due to some multi-threading-related complexity.),
            # resulting in losing logs.
            use_queues=False,
        )

    def _render_filename(self, filename: str) -> str:
        filename = os.path.basename(filename)

        if self.filename_jinja_template:
            formatted_filename = self.filename_jinja_template.render(filename=filename)
        elif self.stream_name_template:
            formatted_filename = self.stream_name_template.format(filename=filename)
        else:
            # Not expected to be run, but covering all bases.
            formatted_filename = filename

        return "scheduler_" + formatted_filename

    def _print(self, msg: str):
        # The DAG processing loggers are not started in the same way that the Web
        # Server, Scheduler, Worker are (which are standalone processes we start and
        # control). Instead, the DAG Processor is started from within Airflow code.  All
        # of the output from that process is captured and fed to logging. So if the
        # logger itself emits logs, it creates a cycle.
        pass


class SubprocessLogHandler(BaseLogHandler):
    """
    A log handler for logs generated by subprocesses we run.

    This handler is used when standard Python logging mechanisms are not directly
    applicable, such as for logs from scheduler, worker, and other similar components.
    In such scenarios, we create a sub-process, capture its stdout and stderr, and push
    them to CloudWatch Logs. In contrast, for logs related to task execution or DAG
    processing, Airflow has a dedicated logger name that we can just define a logger
    for and we are all set.

    Hence, in summary, if we want to capture logs which are known to be generated via
    some Python loggers, then we shouldn't use this class, and instead out for one of
    the other classes in this module, or create a new one. If, however, we want to
    capture logs for a subprocess, e.g. scheduler, then we need to use this handler.
    """

    def __init__(
        self,
        log_group_arn: str,
        kms_key_arn: str,
        stream_name_prefix: str,
        logs_source: str,
        enabled: bool,
    ):
        """
        Initialize the instance.

        Arguments:
            log_group_arn - The ARN of the log group where logs will be published.
            kms_key_arn - The ARN of the KMS key to use when creating the log group
              if necessary.
            stream_name_prefix - The template to use for generating the stream name.
              Currently, in the config.py file, we pass the
              "[logging] LOG_PROCESSOR_FILENAME_TEMPLATE" Airflow configuration [1].
            logs_source - A string identifying the source the logs are coming from, e.g.
              "scheduler". This is used when publishing metrics about logging.
            enabled - Whether this handler is actually enabled, or just does nothing.
              This makes it easier to control enabling and disabling logging without
              much changes to the logging configuration.
        """
        super().__init__(log_group_arn, kms_key_arn, enabled)
        hostname = socket.gethostname()
        epoch = time.time()
        # Use hostname and epoch timestamp as a combined primary key for stream name.
        # The hostname is very helpful for mapping between task logs and the worker that
        # executed the task. But the ECS Fargate hostnames (which are just an IP) are
        # not guaranteed unique and may be reused so include an epoch for uniqueness and
        # easy sorting chronologically.
        _stream_name = "%s_%s_%s.log" % (stream_name_prefix, hostname, epoch)
        self.create_watchtower_handler(_stream_name, logs_source)
