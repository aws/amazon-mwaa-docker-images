import logging
import os

from airflow.decorators import dag
from pendulum import datetime, DateTime

from above.common.constants import (
    S3_CONN_ID,
    S3_DATALAKE_BUCKET,
    TABLEAU_BACKUP_BUCKET_DIRECTORY,
    TABLEAU_SERVER_URL,
    TABLEAU_SITE_ID,
)
from tableau.utils.tableau_utils import get_tableau_dag_default_args
from tableau.operators.tableau_operator import TableauOperator

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
dag_start_date: DateTime = datetime(2024, 6, 1, tz="UTC")


@dag(
    dag_id=this_filename,
    description="Backs up Tableau workbooks updated in the last 24 hours to S3",
    tags=["data", "tableau", "backup"],
    schedule="20 02 * * *",  # Daily 9:20pm CST (02:20 UTC)
    start_date=dag_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=get_tableau_dag_default_args(dag_start_date),
)
def tableau_backup() -> None:
    """
    DAG to backup Tableau workbooks updated in the last 24 hours.

    Connects to Tableau Server API, downloads recently updated workbooks,
    and uploads them to S3 for archival purposes.
    """
    # Create backup task - credentials are auto-retrieved by the operator.
    backup_task: TableauOperator = TableauOperator(
        task_id="backup_tableau_updated_recently",
        updated_since=r"{{ data_interval_start.subtract(hours=24)"
        + ".strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        site_id=TABLEAU_SITE_ID,
        server_url=TABLEAU_SERVER_URL,
        s3_bucket=S3_DATALAKE_BUCKET,
        s3_directory=TABLEAU_BACKUP_BUCKET_DIRECTORY,
        s3_conn_id=S3_CONN_ID,
    )

    # Return the task (or use it in task dependencies)
    backup_task


tableau_backup()
