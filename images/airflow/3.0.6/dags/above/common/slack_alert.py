from typing import Any

from airflow.models import TaskInstance
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.context import Context


def task_failure_slack_alert(context: Context) -> None:

    task_instance: TaskInstance = context.get("task_instance")
    timestamp: str = context.get("ts")

    slack_message_data: dict[str, str] = dict(
        dag=task_instance.dag_id,
        task=task_instance.task_id,
        timestamp=timestamp,
        log_url=task_instance.log_url,
    )

    slack_message_template: str = """
            :red_circle: Task Failed
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {timestamp}
            *Log*: <{log_url}|click here>
            """

    slack_message: str = slack_message_template.format(**slack_message_data)

    failed_alert: SlackWebhookOperator = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_webhook",
        message=slack_message,
    )

    failed_alert.execute(context)
