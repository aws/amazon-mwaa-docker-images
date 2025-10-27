from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Pool
from sqlalchemy.orm import sessionmaker
from airflow.settings import engine
from airflow.operators.bash import BashOperator

def create_pool(pool_name, slots, description,includedeferred):
    include_deferred = includedeferred == "True"
    session = sessionmaker(bind=engine)()
    if session.query(Pool).filter(Pool.pool == pool_name).first() is None:
        new_pool = Pool(pool=pool_name, slots=slots, description=description, include_deferred = include_deferred)
        session.add(new_pool)
        session.commit()
    session.close()

def update_pool(pool_name, slots, description,includedeferred):
    include_deferred = includedeferred == "True"
    session = sessionmaker(bind=engine)()
    pool = session.query(Pool).filter(Pool.pool == pool_name).first()
    if pool:
        pool.slots = slots
        pool.description = description
        pool.include_deferred = include_deferred
        session.commit()
    session.close()

def delete_pool(pool_name):
    session = sessionmaker(bind=engine)()
    pool = session.query(Pool).filter(Pool.pool == pool_name).first()
    if pool:
        session.delete(pool)
        session.commit()
    session.close()

def choose_operation(**kwargs):
    params = kwargs['dag_run'].conf
    operation = params.get('operation')
    if operation == 'create':
        return 'create_pool'
    elif operation == 'update':
        return 'update_pool'
    elif operation == 'delete':
        return 'delete_pool'
    else:
        raise ValueError("Invalid operation specified")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'provide_context': True,
}

with DAG(
    'dynamic_pool_management',
    default_args=default_args,
    description='DAG to dynamically manage Airflow pools based on provided configuration',
    schedule_interval=None, 
    catchup=False,
) as dag:

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_operation,
        provide_context=True,
    )

    create_pool_task = PythonOperator(
        task_id='create_pool',
        python_callable=create_pool,
        op_args=['{{ dag_run.conf["pool_name"] }}', '{{ dag_run.conf["slots"] }}', '{{ dag_run.conf["description"] }}', '{{ dag_run.conf["include_deferred"] }}']
    )

    update_pool_task = PythonOperator(
        task_id='update_pool',
        python_callable=update_pool,
        op_args=['{{ dag_run.conf["pool_name"] }}', '{{ dag_run.conf["slots"] }}', '{{ dag_run.conf["description"] }}', '{{ dag_run.conf["include_deferred"] }}']
    )

    delete_pool_task = PythonOperator(
        task_id='delete_pool',
        python_callable=delete_pool,
        op_args=['{{ dag_run.conf["pool_name"] }}']
    )

    success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
        trigger_rule = 'none_failed',
        dag=dag
    )
    failure = BashOperator(
        task_id='print_failed',
        bash_command='echo "failure"',
        trigger_rule = 'one_failed',
        dag=dag
    )

    branching >> create_pool_task>> [success,failure]
    branching >> update_pool_task >> [success,failure]
    branching >> delete_pool_task >> [success,failure]
