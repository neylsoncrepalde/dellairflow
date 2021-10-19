from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": days_ago(2),
}


@dag(default_args=default_args, schedule_interval="*/5 * * * *", catchup=False, 
    tags=["example", "taskflow", "postgres"], description="Get Data from the internet and manage it on Postgres")
def internet_data_on_postgres():

    get_data = BashOperator(
        task_id="get_data", 
        bash_command="curl https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv -o /usr/local/airflow/data/titanic.csv"
    )


    @task
    def upload_data_to_postgres():
        import pandas as pd
        conn = create_engine("postgresql://airflow:airflow@postgres:5432/postgres")
        df = pd.read_csv("/usr/local/airflow/data/titanic.csv", sep=";")
        df.to_sql("titanic", conn, if_exists="replace", index=False)
        return True


    build_mean_view = PostgresOperator(
        task_id="build_mean_view",
        sql="""
            CREATE OR REPLACE VIEW mean_view AS
            SELECT "Pclass", avg("Age") AS mean_age
            FROM titanic
            GROUP BY "Pclass";
        """,
        postgres_conn_id="my_postgres"
    )

    collected_data = upload_data_to_postgres()
    get_data >> collected_data >> build_mean_view


execution = internet_data_on_postgres()
