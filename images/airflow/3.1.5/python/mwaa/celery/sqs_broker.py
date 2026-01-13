# Copyright (c) 2015-2016 Ask Solem & contributors.  All rights reserved.
# Copyright (c) 2012-2014 GoPivotal Inc & contributors.  All rights reserved.
# Copyright (c) 2009-2012, Ask Solem & contributors.  All rights reserved.
# Modifications Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Ask Solem nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Ask Solem OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""Amazon SQS transport module for Kombu.

This package implements an AMQP-like interface on top of Amazons SQS service,
with the goal of being optimized for high performance and reliability.

The default settings for this module are focused now on high performance in
task queue situations where tasks are small, idempotent and run very fast.

SQS Features supported by this transport
========================================
Long Polling
------------
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html

Long polling is enabled by setting the `wait_time_seconds` transport
option to a number > 1.  Amazon supports up to 20 seconds.  This is
enabled with 10 seconds by default.

Batch API Actions
-----------------
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-batch-api.html

The default behavior of the SQS Channel.drain_events() method is to
request up to the 'prefetch_count' messages on every request to SQS.
These messages are stored locally in a deque object and passed back
to the Transport until the deque is empty, before triggering a new
API call to Amazon.

This behavior dramatically speeds up the rate that you can pull tasks
from SQS when you have short-running tasks (or a large number of workers).

When a Celery worker has multiple queues to monitor, it will pull down
up to 'prefetch_count' messages from queueA and work on them all before
moving on to queueB.  If queueB is empty, it will wait up until
'polling_interval' expires before moving back and checking on queueA.

Other Features supported by this transport
==========================================
Predefined Queues
-----------------
The default behavior of this transport is to use a single AWS credential
pair in order to manage all SQS queues (e.g. listing queues, creating
queues, polling queues, deleting messages).

If it is preferable for your environment to use multiple AWS credentials, you
can use the 'predefined_queues' setting inside the 'transport_options' map.
This setting allows you to specify the SQS queue URL and AWS credentials for
each of your queues. For example, if you have two queues which both already
exist in AWS) you can tell this transport about them as follows:

.. code-block:: python

    transport_options = {
      'predefined_queues': {
        'queue-1': {
          'url': 'https://sqs.us-east-1.amazonaws.com/xxx/aaa',
          'access_key_id': 'a',
          'secret_access_key': 'b',
          'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}, # optional
          'backoff_tasks': ['svc.tasks.tasks.task1'] # optional
        },
        'queue-2.fifo': {
          'url': 'https://sqs.us-east-1.amazonaws.com/xxx/bbb.fifo',
          'access_key_id': 'c',
          'secret_access_key': 'd',
          'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}, # optional
          'backoff_tasks': ['svc.tasks.tasks.task2'] # optional
        },
      }
    'sts_role_arn': 'arn:aws:iam::<xxx>:role/STSTest', # optional
    'sts_token_timeout': 900 # optional
    }

Note that FIFO and standard queues must be named accordingly (the name of
a FIFO queue must end with the .fifo suffix).

backoff_policy & backoff_tasks are optional arguments. These arguments
automatically change the message visibility timeout, in order to have
different times between specific task retries. This would apply after
task failure.

AWS STS authentication is supported, by using sts_role_arn, and
sts_token_timeout. sts_role_arn is the assumed IAM role ARN we are trying
to access with. sts_token_timeout is the token timeout, defaults (and minimum)
to 900 seconds. After the mentioned period, a new token will be created.



If you authenticate using Okta_ (e.g. calling |gac|_), you can also specify
a 'session_token' to connect to a queue. Note that those tokens have a
limited lifetime and are therefore only suited for short-lived tests.

.. _Okta: https://www.okta.com/
.. _gac: https://github.com/Nike-Inc/gimme-aws-creds#readme
.. |gac| replace:: ``gimme-aws-creds``


Client config
-------------
In some cases you may need to override the botocore config. You can do it
as follows:

.. code-block:: python

    transport_option = {
      'client-config': {
          'connect_timeout': 5,
       },
    }

For a complete list of settings you can adjust using this option see
https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: No
"""  # noqa: E501

import base64
import json
import socket
import string
import uuid
from datetime import datetime
from queue import Empty

from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.serialize import Serializer
from vine import ensure_promise, promise, transform

from kombu.asynchronous import get_event_loop
from kombu.asynchronous.aws.ext import boto3, exceptions, AWSRequest
from kombu.asynchronous.aws.sqs.connection import AsyncSQSConnection
from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.log import get_logger
from kombu.utils import scheduling
from kombu.utils.encoding import bytes_to_str, safe_str
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

from kombu.transport import virtual

# 2022-11-25: Amazon addition.
# Airflow Stats object.
from airflow.stats import Stats
from enum import Enum
from multiprocessing import shared_memory
from threading import Lock
import os

from mwaa.logging.utils import throttle

CELERY_WORKER_TASK_LIMIT = int(os.environ.get("AIRFLOW__CELERY__WORKER_AUTOSCALE", "1,1").split(",")[0])
# End of Amazon addition

logger = get_logger(__name__)

# dots are replaced by dash, dash remains dash, all other punctuation
# replaced by underscore.
CHARS_REPLACE_TABLE = {ord(c): 0x5F for c in string.punctuation if c not in "-_."}
CHARS_REPLACE_TABLE[0x2E] = 0x2D  # '.' -> '-'

#: SQS bulk get supports a maximum of 10 messages at a time.
SQS_MAX_MESSAGES = 10


def maybe_int(x):
    """Try to convert x' to int, or return x' if that fails."""
    try:
        return int(x)
    except ValueError:
        return x


# Monkey patch the implementation of ...
def _create_query_request(self, operation, params, queue_url, method):
    params = params.copy()
    if operation:
        params["Action"] = operation

    # defaults for non-get
    param_payload = {"data": params}
    if method.lower() == "get":
        # query-based opts
        param_payload = {"params": params}

    return AWSRequest(method=method, url=queue_url, **param_payload)


def _create_json_request(self, operation, params, queue_url):
    params = params.copy()
    params["QueueUrl"] = queue_url

    service_model = self.sqs_connection.meta.service_model
    operation_model = service_model.operation_model(operation)

    url = self.sqs_connection._endpoint.host

    headers = {}
    # Content-Type
    json_version = operation_model.metadata["jsonVersion"]
    content_type = f"application/x-amz-json-{json_version}"
    headers["Content-Type"] = content_type

    # X-Amz-Target
    target = "{}.{}".format(
        operation_model.metadata["targetPrefix"],
        operation_model.name,
    )
    headers["X-Amz-Target"] = target

    param_payload = {"data": json.dumps(params), "headers": headers}

    method = operation_model.http.get("method", Serializer.DEFAULT_METHOD)
    return AWSRequest(method=method, url=url, **param_payload)


def make_request(self, operation_name, params, queue_url, verb, callback=None, protocol_params=None):
    """
    Overide make_request to support different protocols.

    botocore is soon going to change the default protocol of communicating
    with SQS backend from 'query' to 'json', so we need a special
    implementation of make_request for SQS. More information on this can
    be found in: https://github.com/celery/kombu/pull/1807.
    """
    signer = self.sqs_connection._request_signer

    service_model = self.sqs_connection.meta.service_model
    protocol = service_model.protocol

    if protocol == "query":
        request = self._create_query_request(operation_name, params, queue_url, verb)
    elif protocol == "json":
        request = self._create_json_request(operation_name, params, queue_url)
    else:
        raise Exception(f"Unsupported protocol: {protocol}.")

    signing_type = "presign-url" if request.method.lower() == "get" else "standard"

    signer.sign(operation_name, request, signing_type=signing_type)
    prepared_request = request.prepare()

    return self._mexe(prepared_request, callback=callback)


# Override the implementation of make_request to bring the fix in this PR:
# https://github.com/celery/kombu/pull/1807
AsyncSQSConnection._create_query_request = _create_query_request
AsyncSQSConnection._create_json_request = _create_json_request
AsyncSQSConnection.make_request = make_request


class UndefinedQueueException(Exception):
    """Predefined queues are being used and an undefined queue was used."""


class InvalidQueueException(Exception):
    """Predefined queues are being used and configuration is not valid."""


class QoS(virtual.QoS):
    """Quality of Service guarantees implementation for SQS."""

    def reject(self, delivery_tag, requeue=False):
        super().reject(delivery_tag, requeue=requeue)
        routing_key, message, backoff_tasks, backoff_policy = (
            self._extract_backoff_policy_configuration_and_message(delivery_tag)
        )
        if routing_key and message and backoff_tasks and backoff_policy:
            self.apply_backoff_policy(
                routing_key, delivery_tag, backoff_policy, backoff_tasks
            )

    def _extract_backoff_policy_configuration_and_message(self, delivery_tag):
        try:
            message = self._delivered[delivery_tag]
            routing_key = message.delivery_info["routing_key"]
        except KeyError:
            return None, None, None, None
        if not routing_key or not message:
            return None, None, None, None
        queue_config = self.channel.predefined_queues.get(routing_key, {})
        backoff_tasks = queue_config.get("backoff_tasks")
        backoff_policy = queue_config.get("backoff_policy")
        return routing_key, message, backoff_tasks, backoff_policy

    def apply_backoff_policy(
        self, routing_key, delivery_tag, backoff_policy, backoff_tasks
    ):
        queue_url = self.channel._queue_cache[routing_key]
        task_name, number_of_retries = self.extract_task_name_and_number_of_retries(
            delivery_tag
        )
        if not task_name or not number_of_retries:
            return None
        policy_value = backoff_policy.get(number_of_retries)
        if task_name in backoff_tasks and policy_value is not None:
            c = self.channel.sqs(routing_key)
            c.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=delivery_tag,
                VisibilityTimeout=policy_value,
            )

    def extract_task_name_and_number_of_retries(self, delivery_tag):
        message = self._delivered[delivery_tag]
        message_headers = message.headers
        task_name = message_headers["task"]
        number_of_retries = int(
            message.properties["delivery_info"]["sqs_message"]["Attributes"][
                "ApproximateReceiveCount"
            ]
        )
        return task_name, number_of_retries


class Channel(virtual.Channel):
    """SQS Channel."""

    default_region = "us-east-1"
    default_visibility_timeout = 1800  # 30 minutes.
    default_wait_time_seconds = 10  # up to 20 seconds max
    domain_format = "kombu%(vhost)s"
    _asynsqs = None
    _predefined_queue_async_clients = {}  # A client for each predefined queue
    _sqs = None
    _predefined_queue_clients = {}  # A client for each predefined queue
    _queue_cache = {}
    _noack_queues = set()
    QoS = QoS

    eof_token = "EOF_TOKEN"
    # The SQS channel needs to maintain data regarding the SQS messages that it is currently consuming. This data can be used by the
    # MWAA worker task monitor to check if a worker is idle or not. The data is stored in the shared memory blocks defined below.
    # The shared memory blocks will have a definite size which is calculated here.
    # This per tasks buffer size allows us to store data for each incoming SQS message like the airflow task command contained inside
    # and the SQS message receipt handle. The airflow task command helps with correlating an SQS message with its corresponding Airflow
    # task process and the receipt handle helps with sending the SQS message back to the queue if needed.
    # Furthermore, if celery fails to remove the message from the queue, the message data will still be present in the shared memory blocks
    # defined below. This limit will provide the needed flexibility to go beyond what is actually needed for the happy case scenario and
    # allow the cleanup process to remove the data from the shared memory blocks without running out of space.
    buffer_size_per_task = 2500
    celery_worker_task_limit = int(
        os.environ.get("AIRFLOW__CELERY__WORKER_AUTOSCALE", "20,20").split(",")[0]
    )
    celery_tasks_buffer_size = celery_worker_task_limit * buffer_size_per_task

    # A simple enum to define the type of operations that can be carried out when updating the celery state (memory block containing
    # the current in-flight tasks related data).
    class CeleryStateUpdateAction(Enum):
        # Add data specific to a single Airflow task to the celery state.
        ADD = 1
        # Remove data specific to a single Airflow task from the celery state.
        REMOVE = 2

    def __init__(self, *args, **kwargs):
        if boto3 is None:
            raise ImportError("boto3 is not installed")
        super().__init__(*args, **kwargs)
        self._validate_predifined_queues()

        # SQS blows up if you try to create a new queue when one already
        # exists but with a different visibility_timeout.  This prepopulates
        # the queue_cache to protect us from recreating
        # queues that are known to already exist.
        self._update_queue_cache(self.queue_name_prefix)

        self.hub = kwargs.get("hub") or get_event_loop()

        # MWAA__CORE__TASK_MONITORING_ENABLED is set to 'true' for workers where we want to monitor count of tasks currently getting
        # executed on the worker. This will be used to determine if idle worker checks are to be enabled.
        self.idle_worker_monitoring_enabled = (
            os.environ.get("MWAA__CORE__TASK_MONITORING_ENABLED", "false") == "true"
        )
        if self.idle_worker_monitoring_enabled:
            logger.info('Idle working monitoring will be enabled because '
                        'MWAA__CORE__TASK_MONITORING_ENABLED is set to true.')

        # These are the shared memory blocks which the Worker Task Monitor and the Celery SQS Channel uses to share the internal
        # state of current work load across the two processes.
        # 'celery_state' is maintained by the SQS channel and has information about the current in-flight tasks.
        celery_state_block_name = f'celery_state_{os.environ.get("AIRFLOW_ENV_ID", "")}'
        self.celery_state = (
            shared_memory.SharedMemory(name=celery_state_block_name)
            if self.idle_worker_monitoring_enabled
            else None
        )
        # Create a shared memory block which the Worker Task Monitor and the Celery SQS Channel will use to signal the toggle of a
        # flag which tells the Celery SQS channel to pause/unpause further consumption of available SQS messages.
        # It is maintained by the worker monitor.
        celery_work_consumption_block_name = (
            f'celery_work_consumption_{os.environ.get("AIRFLOW_ENV_ID", "")}'
        )
        self.celery_work_consumption_flag_block = (
            shared_memory.SharedMemory(name=celery_work_consumption_block_name)
            if self.idle_worker_monitoring_enabled
            else None
        )
        # 'cleanup_celery_state' is maintained by the Worker Task Monitor and has information about the current in-flight tasks
        # which needs to be cleaned up from 'celery_state'. The second blob is used because worker task monitor cannot write into
        # 'celery_state'. If worker task monitor was to directly update the 'celery_state', then chances are that changes happening
        # concurrently at the worker task monitor and the SQS channel, can cause changes to be overwritten by one another.
        cleanup_celery_state_block_name = (
            f'cleanup_celery_state_{os.environ.get("AIRFLOW_ENV_ID", "")}'
        )
        self.cleanup_celery_state = (
            shared_memory.SharedMemory(name=cleanup_celery_state_block_name)
            if self.idle_worker_monitoring_enabled
            else None
        )
        self.celery_lock = Lock() if self.idle_worker_monitoring_enabled else None
        # If celery fails to remove the message from the queue but the associated airflow process has wrapped up, then the message details
        # will be stuck in the memory blocks defined above. This is the abandoned sqs messages scenario and this flag determine if we need
        # to intentionally create this scenario for testing purposes.
        self.abandoned_messages_test_enabled = (
            os.environ.get(
                "AIRFLOW__MWAA__TEST_ABANDONED_SQS_MESSAGE_SCENARIOS", "false"
            )
            == "true"
            and self.idle_worker_monitoring_enabled
        )
        # If celery removes the message from the queue but the associated airflow process has not wrapped up, then the process will continue
        # to eat worker resources. This is the undead airflow processes scenario and this flag determine if we need to intentionally create
        # this scenario for testing purposes.
        self.undead_processes_test_enabled = (
            os.environ.get("AIRFLOW__MWAA__TEST_UNDEAD_PROCESS_SCENARIOS", "false")
            == "true"
            and self.idle_worker_monitoring_enabled
        )

    def _get_padded_bytes_from_str(self, raw_data: str):
        data = raw_data + self.eof_token
        data_bytes = bytes(data, "utf-8")
        data_bytes += b"0" * (self.celery_tasks_buffer_size - len(data_bytes))
        return data_bytes

    def _get_str_from_padded_bytes(self, raw_data: bytes):
        data = str(raw_data, "utf-8")
        return data[: data.index(self.eof_token)]

    def _get_tasks_from_state(self, celery_state):
        return loads(
            self._get_str_from_padded_bytes(
                celery_state.buf[: self.celery_tasks_buffer_size]
            )
        )

    def _get_celery_task_index(self, celery_task, celery_tasks):
        for index, task in enumerate(celery_tasks):
            if (
                task["command"] == celery_task["command"]
                and task["receipt_handle"] == celery_task["receipt_handle"]
            ):
                return index
        return -1

    @throttle(seconds=60, log_throttling_msg=False)
    def _report_celery_status_update_no_failure(self):
        # This method is used to report a zero value for the celery_state_update_failure
        # metric. It is throttled with an interval of 60 seconds, to avoid spamming
        # the metric with lots of values.
        Stats.incr("mwaa.celery.celery_state_update_failure", 0)

    def _update_state_with_tasks(
        self, celery_task_tuples, update_action: CeleryStateUpdateAction
    ):
        """
        Update celery_state (memory block containing all in-flight SQS message data) with the provided data related to in-flight SQS
        messages. This method also performs cleanup of celery_state if it finds any task data common between celery_state and
        cleanup_celery_state. Check the definition of cleanup_celery_state above for more details.
        :param celery_task_tuples: List of tuples where each tuple contains the Airflow task command contained in an SQS message being
        consumed by the SQS channel and the SQS message receipt handle.
        :param update_action: Whether to add the provided SQS tasks data to the celery_state (memory block containing all in-flight
        SQS message data) or remove it from the celery_state.
        """
        if self.idle_worker_monitoring_enabled:
            self.celery_lock.acquire()
            try:
                cleanup_celery_tasks = self._get_tasks_from_state(
                    self.cleanup_celery_state
                )
                current_celery_tasks = self._get_tasks_from_state(self.celery_state)
                for cleanup_celery_task in cleanup_celery_tasks:
                    index_for_cleanup = self._get_celery_task_index(
                        cleanup_celery_task, current_celery_tasks
                    )
                    if index_for_cleanup != -1:
                        current_celery_tasks.pop(index_for_cleanup)

                for command, receipt_handle in celery_task_tuples:
                    celery_task = {"command": command, "receipt_handle": receipt_handle}
                    index_for_update = self._get_celery_task_index(
                        celery_task, current_celery_tasks
                    )
                    if (
                        update_action == self.CeleryStateUpdateAction.ADD
                        and index_for_update == -1
                    ):
                        current_celery_tasks.append(celery_task)
                    elif (
                        update_action == self.CeleryStateUpdateAction.REMOVE
                        and index_for_update != -1
                    ):
                        current_celery_tasks.pop(index_for_update)

                self.celery_state.buf[: self.celery_tasks_buffer_size] = (
                    self._get_padded_bytes_from_str(dumps(current_celery_tasks))
                )
                self._report_celery_status_update_no_failure()
            except Exception:
                Stats.incr("mwaa.celery.celery_state_update_failure", 1)
            finally:
                self.celery_lock.release()

    def _is_task_consumption_paused(self):
        """
        celery_work_consumption_block represents the toggle switch for accepting any more incoming SQS message from the
        celery queue which will be used during the shutdown procedure. If this value of the flag is set to 1, then no more SQS messages
        should be consumed by the SQS channel.
        """
        return (
            self.idle_worker_monitoring_enabled
            and self.celery_work_consumption_flag_block.buf[0] == 1
        )

    def _get_task_command_from_sqs_message(self, encoded_sqs_message_body: str) -> str:
        """
        Decode the SQS message and return the task_instance_id (UUID).
        """
        # Decode
        outer_body = json.loads(base64.b64decode(encoded_sqs_message_body))
        inner_body = json.loads(base64.b64decode(outer_body["body"]))

        # Extract args → first element → JSON string
        task_payload_str = inner_body[0][0]
        task_payload = json.loads(task_payload_str)

        # Extract task_instance_id from ti.id
        task_instance_id = task_payload["ti"]["id"]
        return task_instance_id

    def _validate_predifined_queues(self):
        """Check that standard and FIFO queues are named properly.

        AWS requires FIFO queues to have a name
        that ends with the .fifo suffix.
        """
        for queue_name, q in self.predefined_queues.items():
            fifo_url = q["url"].endswith(".fifo")
            fifo_name = queue_name.endswith(".fifo")
            if fifo_url and not fifo_name:
                raise InvalidQueueException(
                    "Queue with url '{}' must have a name " "ending with .fifo".format(
                        q["url"]
                    )
                )
            elif not fifo_url and fifo_name:
                raise InvalidQueueException(
                    "Queue with name '{}' is not a FIFO queue: " "'{}'".format(
                        queue_name, q["url"]
                    )
                )

    def _update_queue_cache(self, queue_name_prefix):
        if self.predefined_queues:
            for queue_name, q in self.predefined_queues.items():
                self._queue_cache[queue_name] = q["url"]
            return

        resp = self.sqs().list_queues(QueueNamePrefix=queue_name_prefix)
        for url in resp.get("QueueUrls", []):
            queue_name = url.split("/")[-1]
            self._queue_cache[queue_name] = url

    # 2025-08-04: Amazon addition.
    # basic_publish is used to publish messages from celery to broker
    def basic_publish(self, message, exchange, routing_key, **kwargs):
        if os.environ.get('MWAA__HEALTH_MONITORING_ENABLE_REVAMPED_HEALTHCHECK', 'false') == 'true' and exchange == 'celeryev':
            # This branch catches all celery task events and generates process heartbeat metrics
            if routing_key == 'worker.heartbeat':
                Stats.gauge("mwaa.celery.process.heartbeat", 1)
                num_active_tasks = len(self._get_tasks_from_state(self.celery_state)) if self.idle_worker_monitoring_enabled else CELERY_WORKER_TASK_LIMIT
                if num_active_tasks >= CELERY_WORKER_TASK_LIMIT:
                    Stats.gauge("mwaa.celery.at_max_concurrency", num_active_tasks)
                if self._is_task_consumption_paused():
                    Stats.gauge("mwaa.celery.sqs.consumption_paused", 1)

            return
        return super().basic_publish(message, exchange, routing_key, **kwargs)
    # End of Amazon addition.

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)
        if self.hub:
            self._loop1(queue)
        return super().basic_consume(queue, no_ack, *args, **kwargs)

    def basic_cancel(self, consumer_tag):
        if consumer_tag in self._consumers:
            queue = self._tag_to_queue[consumer_tag]
            self._noack_queues.discard(queue)
        return super().basic_cancel(consumer_tag)

    def drain_events(self, timeout=None, callback=None, **kwargs):
        """Return a single payload message from one of our queues.

        Raises:
            Queue.Empty: if no messages available.
        """
        # If we're not allowed to consume or have no consumers, raise Empty
        if not self._consumers or not self.qos.can_consume():
            raise Empty()

        # At this point, go and get more messages from SQS
        self._poll(self.cycle, callback, timeout=timeout)

    def _reset_cycle(self):
        """Reset the consume cycle.

        Returns:
            FairCycle: object that points to our _get_bulk() method
                rather than the standard _get() method.  This allows for
                multiple messages to be returned at once from SQS (
                based on the prefetch limit).
        """
        self._cycle = scheduling.FairCycle(
            self._get_bulk,
            self._active_queues,
            Empty,
        )

    def entity_name(self, name, table=CHARS_REPLACE_TABLE):
        """Format AMQP queue name into a legal SQS queue name."""
        if name.endswith(".fifo"):
            partial = name[: -len(".fifo")]
            partial = str(safe_str(partial)).translate(table)
            return partial + ".fifo"
        else:
            return str(safe_str(name)).translate(table)

    def canonical_queue_name(self, queue_name):
        return self.entity_name(self.queue_name_prefix + queue_name)

    def _new_queue(self, queue, **kwargs):
        """Ensure a queue with given name exists in SQS."""
        if not isinstance(queue, str):
            return queue
        # Translate to SQS name for consistency with initial
        # _queue_cache population.
        queue = self.canonical_queue_name(queue)

        # The SQS ListQueues method only returns 1000 queues.  When you have
        # so many queues, it's possible that the queue you are looking for is
        # not cached.  In this case, we could update the cache with the exact
        # queue name first.
        if queue not in self._queue_cache:
            self._update_queue_cache(queue)
        try:
            return self._queue_cache[queue]
        except KeyError:
            if self.predefined_queues:
                raise UndefinedQueueException(
                    (
                        "Queue with name '{}' must be "
                        "defined in 'predefined_queues'."
                    ).format(queue)
                )

            attributes = {"VisibilityTimeout": str(self.visibility_timeout)}
            if queue.endswith(".fifo"):
                attributes["FifoQueue"] = "true"

            resp = self._create_queue(queue, attributes)
            self._queue_cache[queue] = resp["QueueUrl"]
            return resp["QueueUrl"]

    def _create_queue(self, queue_name, attributes):
        """Create an SQS queue with a given name and nominal attributes."""
        # Allow specifying additional boto create_queue Attributes
        # via transport options
        if self.predefined_queues:
            return None

        attributes.update(
            self.transport_options.get("sqs-creation-attributes") or {},
        )

        return self.sqs(queue=queue_name).create_queue(
            QueueName=queue_name,
            Attributes=attributes,
        )

    def _delete(self, queue, *args, **kwargs):
        """Delete queue by name."""
        if self.predefined_queues:
            return
        super()._delete(queue)
        self._queue_cache.pop(queue, None)

    def _put(self, queue, message, **kwargs):
        """Put message onto queue."""
        q_url = self._new_queue(queue)
        if self.sqs_base64_encoding:
            body = AsyncMessage().encode(dumps(message))
        else:
            body = dumps(message)
        kwargs = {"QueueUrl": q_url, "MessageBody": body}
        if queue.endswith(".fifo"):
            if "MessageGroupId" in message["properties"]:
                kwargs["MessageGroupId"] = message["properties"]["MessageGroupId"]
            else:
                kwargs["MessageGroupId"] = "default"
            if "MessageDeduplicationId" in message["properties"]:
                kwargs["MessageDeduplicationId"] = message["properties"][
                    "MessageDeduplicationId"
                ]
            else:
                kwargs["MessageDeduplicationId"] = str(uuid.uuid4())

        c = self.sqs(queue=self.canonical_queue_name(queue))
        if message.get("redelivered"):
            # 2022-11-25: Amazon addition.
            # This branch is executed when a task is returned to the queue, e.g.
            # a worker shutdown:
            # https://github.com/celery/kombu/blob/v4.6.11/kombu/transport/virtual/base.py#L732
            Stats.incr("mwaa.celery.task_returned", 1)
            self._update_state_with_tasks(
                [
                    (
                        self._get_task_command_from_sqs_message(body),
                        message["properties"]["delivery_tag"],
                    )
                ],
                self.CeleryStateUpdateAction.REMOVE,
            )
            # End of Amazon addition
            c.change_message_visibility(
                QueueUrl=q_url,
                ReceiptHandle=message["properties"]["delivery_tag"],
                VisibilityTimeout=0,
            )
        else:
            # 2022-11-25: Amazon addition.
            # This branch is executed when the scheduler puts a task in the
            # queue so it can be picked by a Celery worker.
            Stats.incr("mwaa.celery.task_queued", 1)
            # End of Amazon addition
            c.send_message(**kwargs)

    @staticmethod
    def _optional_b64_decode(byte_string):
        try:
            data = base64.b64decode(byte_string)
            if base64.b64encode(data) == byte_string:
                return data
            # else the base64 module found some embedded base64 content
            # that should be ignored.
        except Exception:  # pylint: disable=broad-except
            pass
        return byte_string

    def _message_to_python(self, message, queue_name, queue):
        body = self._optional_b64_decode(message["Body"].encode())
        payload = loads(bytes_to_str(body))
        if queue_name in self._noack_queues:
            queue = self._new_queue(queue_name)
            # 2022-11-25: Amazon addition.
            # This branch won't be called with our current configuration for
            # Airflow/Celery. It will be called only when task_acks_late Celery
            # configuration is set to False, resulting in task messages being
            # deleted from the SQS immediately, rather than waiting for the
            # worker to finish the task. However, we still report a metric to
            # make sure the code is future-proof, e.g. if Airflow or MWAA decide
            # to disable the task_acks_late configuration.
            Stats.incr("mwaa.celery.task_pulled", 1)
            self._update_state_with_tasks(
                [
                    (
                        self._get_task_command_from_sqs_message(message["Body"]),
                        message["ReceiptHandle"],
                    )
                ],
                self.CeleryStateUpdateAction.ADD,
            )
            # End of Amazon addition.
            self.asynsqs(queue=queue_name).delete_message(
                queue,
                message["ReceiptHandle"],
            )
        else:
            try:
                properties = payload["properties"]
                delivery_info = payload["properties"]["delivery_info"]
            except KeyError:
                # json message not sent by kombu?
                delivery_info = {}
                properties = {"delivery_info": delivery_info}
                payload.update(
                    {
                        "body": bytes_to_str(body),
                        "properties": properties,
                    }
                )
            # set delivery tag to SQS receipt handle
            delivery_info.update(
                {
                    "sqs_message": message,
                    "sqs_queue": queue,
                }
            )
            properties["delivery_tag"] = message["ReceiptHandle"]
        return payload

    def _messages_to_python(self, messages, queue):
        """Convert a list of SQS Message objects into Payloads.

        This method handles converting SQS Message objects into
        Payloads, and appropriately updating the queue depending on
        the 'ack' settings for that queue.

        Arguments:
            messages (SQSMessage): A list of SQS Message objects.
            queue (str): Name representing the queue they came from.

        Returns:
            List: A list of Payload objects
        """
        q = self._new_queue(queue)
        return [self._message_to_python(m, queue, q) for m in messages]

    def _get_bulk(self, queue, max_if_unlimited=SQS_MAX_MESSAGES, callback=None):
        """Try to retrieve multiple messages off ``queue``.

        Where :meth:`_get` returns a single Payload object, this method
        returns a list of Payload objects.  The number of objects returned
        is determined by the total number of messages available in the queue
        and the number of messages the QoS object allows (based on the
        prefetch_count).

        Note:
            Ignores QoS limits so caller is responsible for checking
            that we are allowed to consume at least one message from the
            queue.  get_bulk will then ask QoS for an estimate of
            the number of extra messages that we can consume.

        Arguments:
            queue (str): The queue name to pull from.

        Returns:
            List[Message]
        """
        # drain_events calls `can_consume` first, consuming
        # a token, so we know that we are allowed to consume at least
        # one message.

        # Note: ignoring max_messages for SQS with boto3
        max_count = self._get_message_estimate()
        if max_count:
            q_url = self._new_queue(queue)

            # 2022-11-25: Amazon addition.
            # Send a heartbeat metric each time we try to receive messages from SQS.
            # I didn't notice this branch being executed as it seems that async client
            # is used in our case, but adding this nevertheless to be future-proof.
            Stats.incr("mwaa.celery.heartbeat", 1)
            # End of Amazon addition.

            resp = self.sqs(queue=queue).receive_message(
                QueueUrl=q_url,
                MaxNumberOfMessages=max_count,
                WaitTimeSeconds=self.wait_time_seconds,
            )
            if resp.get("Messages"):
                # 2022-11-25: Amazon addition.
                # Since we pulled some messages from the SQS queue, we report
                # a metric indicating the number of tasks pulled for execution.
                # I didn't notice this branch being executed as it seems that
                # async client is used in our case, but adding this nevertheless
                # to be future-proof.
                Stats.incr("mwaa.celery.task_pulled", len(resp.get("Messages")))
                celery_task_tuples = []
                # End of Amazon addition.

                for m in resp["Messages"]:
                    celery_task_tuples.append(
                        (
                            self._get_task_command_from_sqs_message(m["Body"]),
                            m["ReceiptHandle"],
                        )
                    )
                    m["Body"] = AsyncMessage(body=m["Body"]).decode()
                self._update_state_with_tasks(
                    celery_task_tuples, self.CeleryStateUpdateAction.ADD
                )
                for msg in self._messages_to_python(resp["Messages"], queue):
                    self.connection._deliver(msg, queue)
                return
        raise Empty()

    def _get(self, queue):
        """Try to retrieve a single message off ``queue``."""
        q_url = self._new_queue(queue)

        # 2022-11-25: Amazon addition.
        # Send a heartbeat metric each time we try to receive messages from SQS.
        # I didn't notice this branch being executed as it seems that async client
        # is used in our case, but adding this nevertheless to be future-proof.
        Stats.incr("mwaa.celery.heartbeat", 1)

        # If worker monitoring is enabled and the worker has been told to pause consumption, then we return Empty here to
        # simulate the situation where there are no messages in the celery queue.
        if self._is_task_consumption_paused():
            raise Empty()
        # End of Amazon addition.

        resp = self.sqs(queue=queue).receive_message(
            QueueUrl=q_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=self.wait_time_seconds,
        )
        if resp.get("Messages"):
            # 2022-11-25: Amazon addition.
            # Since we pulled a message from the SQS queue, we report a metric
            # indicating that.
            # I didn't notice this branch being executed as it seems that async client
            # is used in our case, but adding this nevertheless to be future-proof.
            Stats.incr("mwaa.celery.task_pulled", 1)
            self._update_state_with_tasks(
                [
                    (
                        self._get_task_command_from_sqs_message(
                            resp["Messages"][0]["Body"]
                        ),
                        resp["Messages"][0]["ReceiptHandle"],
                    )
                ],
                self.CeleryStateUpdateAction.ADD,
            )
            # End of Amazon addition.

            body = AsyncMessage(body=resp["Messages"][0]["Body"]).decode()
            resp["Messages"][0]["Body"] = body
            return self._messages_to_python(resp["Messages"], queue)[0]
        raise Empty()

    def _loop1(self, queue, _=None):
        self.hub.call_soon(self._schedule_queue, queue)

    def _schedule_queue(self, queue):
        if queue in self._active_queues:
            if self.qos.can_consume():
                self._get_bulk_async(
                    queue,
                    callback=promise(self._loop1, (queue,)),
                )
            else:
                self._loop1(queue)

    def _get_message_estimate(self, max_if_unlimited=SQS_MAX_MESSAGES):
        # 2024-02-01: Amazon addition.
        # If worker monitoring is enabled and the worker has been told to pause consumption, then we return zero here to
        # simulate the situation where there are no messages in the celery queue.
        if self._is_task_consumption_paused():
            return 0
        # End of Amazon addition.

        maxcount = self.qos.can_consume_max_estimate()
        return min(
            max_if_unlimited if maxcount is None else max(maxcount, 1),
            max_if_unlimited,
        )

    def _get_bulk_async(self, queue, max_if_unlimited=SQS_MAX_MESSAGES, callback=None):
        maxcount = self._get_message_estimate()
        if maxcount:
            return self._get_async(queue, maxcount, callback=callback)
        # Not allowed to consume, make sure to notify callback..
        callback = ensure_promise(callback)
        callback([])
        return callback

    def _get_async(self, queue, count=1, callback=None):
        q = self._new_queue(queue)
        qname = self.canonical_queue_name(queue)
        return self._get_from_sqs(
            qname,
            count=count,
            connection=self.asynsqs(queue=qname),
            callback=transform(self._on_messages_ready, callback, q, queue),
        )

    def _on_messages_ready(self, queue, qname, messages):
        if "Messages" in messages and messages["Messages"]:
            callbacks = self.connection._callbacks

            # 2022-11-25: Amazon addition.
            # This code is called after messages are retrieved from SQS via
            # the async client. From our perspective, this indicates tasks
            # pulled from the queue, so we report a metric.
            Stats.incr("mwaa.celery.task_pulled", len(messages["Messages"]))
            celery_task_tuples = []

            for msg in messages["Messages"]:
                celery_task_tuples.append(
                    (
                        self._get_task_command_from_sqs_message(msg["Body"]),
                        msg["ReceiptHandle"],
                    )
                )
                msg_parsed = self._message_to_python(msg, qname, queue)
                callbacks[qname](msg_parsed)

            should_add_all_messages = True
            if self.undead_processes_test_enabled and len(celery_task_tuples) > 1:
                # In order to test for undead processes, we will not add the data regarding the first message fetched from the queue
                # in 50% of the cases. This will cause the cleanup process to treat the corresponding Airflow process as undead and that
                # Airflow process will be terminated.
                import random

                if random.randint(0, 9) < 5:
                    should_add_all_messages = False

            if should_add_all_messages:
                self._update_state_with_tasks(
                    celery_task_tuples, self.CeleryStateUpdateAction.ADD
                )
            else:
                self._update_state_with_tasks(
                    celery_task_tuples[1:], self.CeleryStateUpdateAction.ADD
                )
            # End of Amazon addition.

    def _get_from_sqs(self, queue, count=1, connection=None, callback=None):
        """Retrieve and handle messages from SQS.

        Uses long polling and returns :class:`~vine.promises.promise`.
        """
        connection = connection if connection is not None else queue.connection
        if self.predefined_queues:
            if queue not in self._queue_cache:
                raise UndefinedQueueException(
                    (
                        "Queue with name '{}' must be defined in "
                        "'predefined_queues'."
                    ).format(queue)
                )
            queue_url = self._queue_cache[queue]
        else:
            queue_url = connection.get_queue_url(queue)

        # 2022-11-25: Amazon addition.
        # Send a heartbeat metric each time we try to receive messages from SQS.
        Stats.incr("mwaa.celery.heartbeat", 1)
        # End of Amazon addition.

        return connection.receive_message(
            queue,
            queue_url,
            number_messages=count,
            wait_time_seconds=self.wait_time_seconds,
            callback=callback,
        )

    def _restore(self, message, unwanted_delivery_info=("sqs_message", "sqs_queue")):
        for unwanted_key in unwanted_delivery_info:
            # Remove objects that aren't JSON serializable (Issue #1108).
            message.delivery_info.pop(unwanted_key, None)
        return super()._restore(message)

    def basic_ack(self, delivery_tag, multiple=False):
        try:
            message = self.qos.get(delivery_tag).delivery_info
            sqs_message = message["sqs_message"]
        except KeyError:
            super().basic_ack(delivery_tag)
        else:
            queue = None
            if "routing_key" in message:
                queue = self.canonical_queue_name(message["routing_key"])

            try:
                # 2022-11-25: Amazon addition.
                # This code is executed when a task finishes execution and
                # thus its message can be removed from the SQS queue. We thus
                # report a task_executed metric.
                Stats.incr("mwaa.celery.task_executed", 1)
                should_remove_sqs_message = True
                celery_task_tuple = (
                    self._get_task_command_from_sqs_message(sqs_message["Body"]),
                    sqs_message["ReceiptHandle"],
                )

                if self.abandoned_messages_test_enabled:
                    # In order to test for abandoned tasks, we will not remove the message from the SQS queue in 50% of the cases.
                    # This will cause the cleanup process to treat the corresponding SQS message as abandoned and the message will be
                    # returned to the queue by setting its visibility timeout to zero.
                    # Also checking that the message data should be present in the internal state (memory blocks) because the
                    # abandoned SQS message test scenario and undead Airflow process test scenario are not possible to occur
                    # simultaneously based on definition.
                    import random

                    celery_task = {
                        "command": celery_task_tuple[0],
                        "receipt_handle": celery_task_tuple[1],
                    }
                    celery_task_index = self._get_celery_task_index(
                        celery_task, self._get_tasks_from_state(self.celery_state)
                    )
                    if random.randint(0, 9) < 5 and celery_task_index != -1:
                        should_remove_sqs_message = False

                if should_remove_sqs_message:
                    self._update_state_with_tasks(
                        [celery_task_tuple], self.CeleryStateUpdateAction.REMOVE
                    )
                    self.sqs(queue=queue).delete_message(
                        QueueUrl=message["sqs_queue"],
                        ReceiptHandle=sqs_message["ReceiptHandle"],
                    )
                # End of Amazon addition.
            except ClientError:
                super().basic_reject(delivery_tag)
            else:
                super().basic_ack(delivery_tag)

    def _size(self, queue):
        """Return the number of messages in a queue."""
        url = self._new_queue(queue)
        c = self.sqs(queue=self.canonical_queue_name(queue))
        resp = c.get_queue_attributes(
            QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        try:
            return int(resp["Attributes"]["ApproximateNumberOfMessages"])
        except Exception:
            logger.error("Unexpected response from SQS get_queue_attributes: %s", resp)
            raise

    def _purge(self, queue):
        """Delete all current messages in a queue."""
        q = self._new_queue(queue)
        # SQS is slow at registering messages, so run for a few
        # iterations to ensure messages are detected and deleted.
        size = 0
        for i in range(10):
            size += int(self._size(queue))
            if not size:
                break
        self.sqs(queue=queue).purge_queue(QueueUrl=q)
        return size

    def close(self):
        # 2024-02-01: Amazon addition.
        # If worker monitoring is enabled, then we make use of shared memory blocks to share the internal state from this
        # SQS Channel with the worker task monitor. When closing the channel, we also close the shared memory blocks in order to
        # prevent a warning message to show up in the logs. We are not calling unlink here intentionally because both the task monitor and
        # the Celery SQS channel references these blocks and when the worker is shutting down, we do not control which process will be
        # killed first. Calling unlink may result in a warning message showing up in the customer side logs causing unnecessary confusion.
        if self.idle_worker_monitoring_enabled:
            self.celery_state.close()
            self.celery_work_consumption_flag_block.close()
            self.cleanup_celery_state.close()
        # End of Amazon addition.

        super().close()
        # if self._asynsqs:
        #     try:
        #         self.asynsqs().close()
        #     except AttributeError as exc:  # FIXME ???
        #         if "can't set attribute" not in str(exc):
        #             raise

    def new_sqs_client(
        self, region, access_key_id, secret_access_key, session_token=None
    ):
        session = boto3.session.Session(
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )
        is_secure = self.is_secure if self.is_secure is not None else True
        client_kwargs = {"use_ssl": is_secure}
        if self.endpoint_url is not None:
            client_kwargs["endpoint_url"] = self.endpoint_url
        client_config = self.transport_options.get("client-config") or {}
        config = Config(**client_config)
        return session.client("sqs", config=config, **client_kwargs)

    def sqs(self, queue=None):
        if queue is not None and self.predefined_queues:
            if queue not in self.predefined_queues:
                raise UndefinedQueueException(
                    f"Queue with name '{queue}' must be defined"
                    " in 'predefined_queues'."
                )
            q = self.predefined_queues[queue]
            if self.transport_options.get("sts_role_arn"):
                return self._handle_sts_session(queue, q)
            if not self.transport_options.get("sts_role_arn"):
                if queue in self._predefined_queue_clients:
                    return self._predefined_queue_clients[queue]
                else:
                    c = self._predefined_queue_clients[queue] = self.new_sqs_client(
                        region=q.get("region", self.region),
                        access_key_id=q.get("access_key_id", self.conninfo.userid),
                        secret_access_key=q.get(
                            "secret_access_key", self.conninfo.password
                        ),
                    )
                    return c

        if self._sqs is not None:
            return self._sqs

        c = self._sqs = self.new_sqs_client(
            region=self.region,
            access_key_id=self.conninfo.userid,
            secret_access_key=self.conninfo.password,
        )
        return c

    def _handle_sts_session(self, queue, q):
        if not hasattr(self, "sts_expiration"):  # STS token - token init
            sts_creds = self.generate_sts_session_token(
                self.transport_options.get("sts_role_arn"),
                self.transport_options.get("sts_token_timeout", 900),
            )
            self.sts_expiration = sts_creds["Expiration"]
            c = self._predefined_queue_clients[queue] = self.new_sqs_client(
                region=q.get("region", self.region),
                access_key_id=sts_creds["AccessKeyId"],
                secret_access_key=sts_creds["SecretAccessKey"],
                session_token=sts_creds["SessionToken"],
            )
            return c
        # STS token - refresh if expired
        elif self.sts_expiration.replace(tzinfo=None) < datetime.utcnow():
            sts_creds = self.generate_sts_session_token(
                self.transport_options.get("sts_role_arn"),
                self.transport_options.get("sts_token_timeout", 900),
            )
            self.sts_expiration = sts_creds["Expiration"]
            c = self._predefined_queue_clients[queue] = self.new_sqs_client(
                region=q.get("region", self.region),
                access_key_id=sts_creds["AccessKeyId"],
                secret_access_key=sts_creds["SecretAccessKey"],
                session_token=sts_creds["SessionToken"],
            )
            return c
        else:  # STS token - ruse existing
            return self._predefined_queue_clients[queue]

    def generate_sts_session_token(self, role_arn, token_expiry_seconds):
        sts_client = boto3.client("sts")
        sts_policy = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="Celery",
            DurationSeconds=token_expiry_seconds,
        )
        return sts_policy["Credentials"]

    def asynsqs(self, queue=None):
        if queue is not None and self.predefined_queues:
            if queue in self._predefined_queue_async_clients and not hasattr(
                self, "sts_expiration"
            ):
                return self._predefined_queue_async_clients[queue]
            if queue not in self.predefined_queues:
                raise UndefinedQueueException(
                    (
                        "Queue with name '{}' must be defined in "
                        "'predefined_queues'."
                    ).format(queue)
                )
            q = self.predefined_queues[queue]
            c = self._predefined_queue_async_clients[queue] = AsyncSQSConnection(
                sqs_connection=self.sqs(queue=queue),
                region=q.get("region", self.region),
            )
            return c

        if self._asynsqs is not None:
            return self._asynsqs

        c = self._asynsqs = AsyncSQSConnection(
            sqs_connection=self.sqs(queue=queue), region=self.region
        )
        return c

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def visibility_timeout(self):
        return (
            self.transport_options.get("visibility_timeout")
            or self.default_visibility_timeout
        )

    @cached_property
    def predefined_queues(self):
        """Map of queue_name to predefined queue settings."""
        return self.transport_options.get("predefined_queues", {})

    @cached_property
    def queue_name_prefix(self):
        return self.transport_options.get("queue_name_prefix", "")

    @cached_property
    def supports_fanout(self):
        return False

    @cached_property
    def region(self):
        return (
            self.transport_options.get("region")
            or boto3.Session().region_name
            or self.default_region
        )

    @cached_property
    def regioninfo(self):
        return self.transport_options.get("regioninfo")

    @cached_property
    def is_secure(self):
        return self.transport_options.get("is_secure")

    @cached_property
    def port(self):
        return self.transport_options.get("port")

    @cached_property
    def endpoint_url(self):
        if self.conninfo.hostname is not None:
            scheme = "https" if self.is_secure else "http"
            if self.conninfo.port is not None:
                port = f":{self.conninfo.port}"
            else:
                port = ""
            return "{}://{}{}".format(scheme, self.conninfo.hostname, port)

    @cached_property
    def wait_time_seconds(self):
        return self.transport_options.get(
            "wait_time_seconds", self.default_wait_time_seconds
        )

    @cached_property
    def sqs_base64_encoding(self):
        return self.transport_options.get("sqs_base64_encoding", True)


class Transport(virtual.Transport):
    """SQS Transport.

    Additional queue attributes can be supplied to SQS during queue
    creation by passing an ``sqs-creation-attributes`` key in
    transport_options. ``sqs-creation-attributes`` must be a dict whose
    key-value pairs correspond with Attributes in the
    `CreateQueue SQS API`_.

    For example, to have SQS queues created with server-side encryption
    enabled using the default Amazon Managed Customer Master Key, you
    can set ``KmsMasterKeyId`` Attribute. When the queue is initially
    created by Kombu, encryption will be enabled.

    .. code-block:: python

        from kombu.transport.SQS import Transport

        transport = Transport(
            ...,
            transport_options={
                'sqs-creation-attributes': {
                    'KmsMasterKeyId': 'alias/aws/sqs',
                },
            }
        )

    .. _CreateQueue SQS API: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#API_CreateQueue_RequestParameters
    """  # noqa: E501

    Channel = Channel

    polling_interval = 1
    wait_time_seconds = 0
    default_port = None
    connection_errors = virtual.Transport.connection_errors + (
        exceptions.BotoCoreError,
        socket.error,
    )
    channel_errors = virtual.Transport.channel_errors + (exceptions.BotoCoreError,)
    driver_type = "sqs"
    driver_name = "sqs"

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(["direct"]),
    )

    @property
    def default_connection_params(self):
        return {"port": self.default_port}
