from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'Neylson Crepalde',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['neylson.crepalde@a3data.com.br'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Define the DAG object using dag decorator
@dag(default_args=default_args, schedule_interval="*/2 * * * *", catchup=False, 
    tags=["example", "taskflow"], description="Example dag with NEW taskflow API")
def my_first_dag():

    @task
    def start():
        print("Hello, World! This is Airflow with the NEW taskflow API.")


    s = start()

execution = my_first_dag()