"""
This module contain multiple log handlers to support integration with CloudWatch Logs.

It contains the BaseLogHandler class, which has some common functionality needed by
all of the handlers. It also contains a couple other handlers for different log
categories, e.g. SubprocessLogHandler for handling scheduler/worker/etc logs, and so on.

For Airflow 3 we added CloudWatchRemoteTaskLogger for task logging only due to the switch to
Structlog. It still inherits BaseLogHandler for metrics reporting purposes but the core logic
are different. e.g. Emit and set_context are no-ops as they are not called anymore, and
the log publishing part is in the processor property.
"""

# Python imports
import contextlib
import copy
from datetime import datetime, timedelta, timezone
from functools import cached_property
import logging
import json
import os
import re
import structlog
import sys
import traceback

# 3rd party imports
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.utils import datetime_to_epoch_utc_ms
from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
from airflow.utils.helpers import parse_template_string, render_template
from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo, LogMetadata
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session, NEW_SESSION
from mypy_boto3_logs.client import CloudWatchLogsClient
from typing import Dict
import boto3
import socket
import time
import watchtower
from fluent import asynchandler as fluent_handler

# Our imports
from mwaa.logging.utils import parse_arn, throttle
from mwaa.utils.statsd import get_statsd

USE_NON_CRITICAL_LOGGING = os.environ.get('USE_NON_CRITICAL_LOGGING', 'false')

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
        self.NON_CRITICAL_LOGGING_ENABLED = USE_NON_CRITICAL_LOGGING.lower() == 'true'
        self.log_stream = None

        # TODO Find a nice and unambiguous solution to the craziness of super() and MRO.
        logging.Handler.__init__(self)

        self.stats = get_statsd()

    def create_cloudwatch_handler(
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

        self.logs_source = logs_source
        self.log_stream = stream_name

        if self.enabled and self.NON_CRITICAL_LOGGING_ENABLED:
            self.handler = fluent_handler.FluentHandler(
                'customer.logs',
                host='localhost',
                port=24224
            )
            if self.formatter:
                # Wrap the existing formatter to add routing fields
                original_formatter = self.formatter

                log_group = self.log_group_name
                log_stream = self.log_stream

                class RoutingFormatter(logging.Formatter):
                    def format(self, record) -> Dict[str, str]:  # type: ignore[override]
                        # Get the original formatted message
                        formatted_msg = original_formatter.format(record)
                        # Return dict with both the original format and routing fields
                        return {
                            'log_group': log_group,
                            'log_stream': log_stream,
                            'message': formatted_msg
                        }
                self.handler.setFormatter(RoutingFormatter())
            else:
                log_group = self.log_group_name
                log_stream = self.log_stream
                # If no formatter exists, use a basic one with routing fields
                class DefaultRoutingFormatter(logging.Formatter):
                    def format(self, record) -> Dict[str, str]:  # type: ignore[override]
                        return {
                            'log_group': log_group,
                            'log_stream': log_stream,
                            'message': record.getMessage()
                        }
                self.handler.setFormatter(DefaultRoutingFormatter())

        elif self.enabled:
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
            if record.levelno >= logging.getLevelName(
                    os.environ.get(f"MWAA__LOGGING__AIRFLOW_{self.logs_source.upper()}_LOG_LEVEL", "INFO")):
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

class CloudWatchRemoteTaskLogger(BaseLogHandler, LoggingMixin):
    """
        In Airflow 3, we need to enable remote logging in order to load our custom logging logic into the task logger
        which uses Structlog. The main logic resides in 'processors' property which is loaded by Structlog logger in
        task-sdk/src/airflow/sdk/log.py#L143
    """
    LOG_SOURCE = "task"
    # In Airflow currently there are some logs that "seeps" out of the traditional task log stream. Ideally we should
    # use log_filename_template config to generate a pattern for filtering only task log streams. But that config
    # value is not a regex pattern but instead a Jinja template. As a workaround for now we use a deny list instead.
    IGNORED_PATTERNS = [
        # Dag processor log (from loading DagBag) with stream name dag_processor/2025-01-01/dags-folder/dag.py.log
        re.compile(r"^dag_processor/")
    ]

    def __init__(
        self,
        log_group_arn: str,
        kms_key_arn: str | None,
        enabled: bool,
        log_level: str
    ):
        BaseLogHandler.__init__(self, log_group_arn, kms_key_arn, enabled)
        self.log_level = logging.getLevelName(log_level)
        self.handler = None

    def get_handler(self):
        if not self.handler:
            self.handler = self._init_handler()
        return self.handler

    def _init_handler(self):
        logs_client: CloudWatchLogsClient = boto3.client("logs")  # type: ignore

        return watchtower.CloudWatchLogHandler(
            log_group_name=self.log_group_name,
            boto3_client=logs_client,
            use_queues=True,
            create_log_group=False,
        )

    @cached_property
    def processors(self) -> tuple[structlog.typing.Processor, ...]:
        """
            This is a (almost) direct port from CloudWatchRemoteLogIO class in Amazon provider. We need to carry out
            the log writing logic in a processor that belongs to the remote logging class in Airflow 3 specifically
            for task logging. In Airflow 3 task logging is done through Structlog instead of the old customer task log
            handlers. And only the processor attribute from the remote logging class is loaded into the Structlog
            logger used for task logging.
        """
        from logging import getLogRecordFactory

        import structlog.stdlib

        logRecordFactory = getLogRecordFactory()
        # The handler MUST be initted here, before the processor is actually used to log anything.
        # Otherwise, logging that occurs during the creation of the handler can create infinite loops.
        _handler = self.get_handler()
        from airflow.sdk.log import relative_path_from_logger

        def proc(logger: structlog.typing.WrappedLogger, method_name: str, event: structlog.typing.EventDict):
            if not logger or not (stream_name := relative_path_from_logger(logger)):
                return event
            _handler.log_stream_name = stream_name.as_posix().replace(":", "_")

            if self.log_group_name.endswith("-Task") \
                    and any(re.match(p, _handler.log_stream_name) for p in CloudWatchRemoteTaskLogger.IGNORED_PATTERNS):
                return event

            name = event.get("logger_name") or event.get("logger", "")
            level = structlog.stdlib.NAME_TO_LEVEL.get(method_name.lower(), logging.INFO)
            if level < self.log_level:
                return event

            msg = copy.copy(event)
            created = None
            if ts := msg.pop("timestamp", None):
                with contextlib.suppress(Exception):
                    created = datetime.fromisoformat(ts)
            record = logRecordFactory(
                name, level, pathname="", lineno=0, msg=msg, args=(), exc_info=None, func=None, sinfo=None
            )
            if created is not None:
                ct = created.timestamp()
                record.created = ct
                record.msecs = int((ct - int(ct)) * 1000) + 0.0  # Copied from stdlib logging
            try:
                _handler.handle(record)
            except Exception as e:
                self.stats.incr(f"mwaa.logging.{CloudWatchRemoteTaskLogger.LOG_SOURCE}.emit_error", 1)
                # TODO maybe consider removing this if we plan to make logging non-critical
                raise e
            return event

        return (proc,)

    def emit(self, record: logging.LogRecord):
        # No-op as the processor will take care of the uploading part. Also since set_context will no longer be called
        # in Airflow 3, we will have no information on the current log stream name here.
        return

    def close(self):
        if self.handler:
            self.handler.close()

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        # No-op, as we upload via the processor as we go
        # But we need to give the handler time to finish off its business
        self.flush()
        return

    def read(
        self, task_instance, try_number, metadata=None
    ) -> tuple[LogMessages, LogMetadata]:
        """
            Invoked by airflow-core/src/airflow/utils/log/log_reader.py when console tries to load task log. This is
            the reason why we set Task logging handler to this class even though the actual log writing is done through
            remote logging processor.
        """
        stream_name = self._render_filename(task_instance, try_number).replace(":", "_")
        messages, logs = self._read_remote_logs(stream_name, task_instance)
        return messages + logs, metadata

    def _read_remote_logs(self, relative_path, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages | None]:
        messages = [
            f"Reading remote log from Cloudwatch log_group: {self.log_group_arn} log_stream: {relative_path}"
        ]
        try:
            from airflow.utils.log.file_task_handler import StructuredLogMessage

            logs = [
                StructuredLogMessage.model_validate(log)
                for log in self.get_cloudwatch_logs(relative_path, ti)
            ]
        except Exception as e:
            logs = None
            messages.append(str(e))

        return messages, logs or []

    @provide_session
    def _render_filename(self, ti: TaskInstance, try_number: int, session=NEW_SESSION) -> str:
        dag_run = ti.get_dagrun(session=session)

        date = dag_run.logical_date or dag_run.run_after
        date = date.isoformat()

        template = dag_run.get_log_template(session=session).filename
        str_tpl, jinja_tpl = parse_template_string(template)
        if jinja_tpl:
            return render_template(jinja_tpl, {"ti": ti, "ts": date, "try_number": try_number}, native=False)

        if str_tpl:
            data_interval = (dag_run.data_interval_start, dag_run.data_interval_end)
            if data_interval[0]:
                data_interval_start = data_interval[0].isoformat()
            else:
                data_interval_start = ""
            if data_interval[1]:
                data_interval_end = data_interval[1].isoformat()
            else:
                data_interval_end = ""
            return str_tpl.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                logical_date=date,
                try_number=try_number,
            )
        raise RuntimeError(f"Unable to render log filename for {ti}. This should never happen")

    @cached_property
    def hook(self):
        """Returns AwsLogsHook."""
        return AwsLogsHook(
            region_name=self.region_name
        )

    def get_cloudwatch_logs(self, stream_name: str, task_instance: RuntimeTI):
        """
        Return all logs from the given log stream.

        :param stream_name: name of the Cloudwatch log stream to get all logs from
        :param task_instance: the task instance to get logs about
        :return: string of all logs from the given log stream
        """
        stream_name = stream_name.replace(":", "_")
        # If there is an end_date to the task instance, fetch logs until that date + 30 seconds
        # 30 seconds is an arbitrary buffer so that we don't miss any logs that were emitted
        end_time = (
            None
            if (end_date := getattr(task_instance, "end_date", None)) is None
            else datetime_to_epoch_utc_ms(end_date + timedelta(seconds=30))
        )
        log_group = self.log_group_arn.rsplit(":", 1)[1]
        events = self.hook.get_log_events(
            log_group=log_group,
            log_stream_name=stream_name,
            end_time=end_time,
        )
        return list(self._event_to_dict(e) for e in events)

    def _event_to_dict(self, event: dict) -> dict:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc).isoformat()
        message = event["message"]
        try:
            message = json.loads(message)
            message["timestamp"] = event_dt
            return message
        except Exception:
            return {"timestamp": event_dt, "event": message}

    def _event_to_str(self, event: dict) -> str:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc)
        formatted_event_dt = event_dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        message = event["message"]
        return f"[{formatted_event_dt}] {message}"


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
        self.create_cloudwatch_handler(stream_name, "DAGProcessorManager")

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
        self.create_cloudwatch_handler(
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
        log_formatter: logging.Formatter | None = None,
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
        self.formatter = log_formatter
        hostname = socket.gethostname()
        epoch = time.time()
        # Use hostname and epoch timestamp as a combined primary key for stream name.
        # The hostname is very helpful for mapping between task logs and the worker that
        # executed the task. But the ECS Fargate hostnames (which are just an IP) are
        # not guaranteed unique and may be reused so include an epoch for uniqueness and
        # easy sorting chronologically.
        _stream_name = "%s_%s_%s.log" % (stream_name_prefix, hostname, epoch)
        self.create_cloudwatch_handler(_stream_name, logs_source)
