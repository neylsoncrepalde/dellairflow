from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": days_ago(2),
}

# Python function that branches male or female
def male_or_female(ti):
    gender = ti.xcom_pull(key='gender', task_ids=["flip_the_coin"])
    if gender == 'male':
        return 'mean_male'
    else:
        return 'mean_female'


@dag(default_args=default_args, schedule_interval="*/3 * * * *", catchup=False, 
    tags=["example", "taskflow", "branching"], description="Example of branching in a Dag")
def branching_example():

    get_data = BashOperator(
        task_id="get_data", 
        bash_command="curl https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv -o /usr/local/airflow/data/titanic.csv"
    )

    @task
    def flip_the_coin():
        import random
        return {
            'gender': random.choice(['male', 'female'])
        }

    
    branch_task = BranchPythonOperator(
        task_id='male_or_female',
        python_callable=male_or_female,
        provide_context=True
    )


    @task
    def mean_female():
        import pandas as pd
        df = pd.read_csv("/usr/local/airflow/data/titanic.csv", sep=";")
        df = df.loc[df.Sex == 'female']
        print(f"Calculated mean Age for women: {df.Age.mean()}")

    
    @task
    def mean_male():
        import pandas as pd
        df = pd.read_csv("/usr/local/airflow/data/titanic.csv", sep=";")
        df = df.loc[df.Sex == 'male']
        print(f"Calculated mean Age for men: {df.Age.mean()}")

      
    
    coin = flip_the_coin()
    res_avg_male = mean_male()
    res_avg_female = mean_female()
    get_data >> coin >> branch_task >> [res_avg_female, res_avg_male]

execution = branching_example()