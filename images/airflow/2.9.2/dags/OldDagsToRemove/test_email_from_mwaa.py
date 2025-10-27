from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(
    'test_dag',
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='Test DAG',
    schedule_interval= None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Test', 'WIP']
) as dag:


    test_task = BashOperator(
    task_id='fail_task',
    bash_command='exit 1',
    #on_failure_callback=slack_failed_task,
    # provide_context=True,
    # dag=dag
    )

    success = BashOperator(
    task_id='print_success',
    bash_command='echo "Success"',
    )

    test_task >> success