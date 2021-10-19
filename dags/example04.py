from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": days_ago(2),
}

@dag(default_args=default_args, schedule_interval="*/3 * * * *", catchup=False, 
    tags=["example", "taskflow", "branching"], description="Example of branching in a Dag")
def branching_example():

    get_data = BashOperator(
        task_id="get_data", 
        bash_command="curl https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv -o /usr/local/airflow/data/titanic.csv"
    )

    # TBC

    get_data

execution = branching_example()