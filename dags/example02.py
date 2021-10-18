from typing import get_args
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

base_path = "/usr/local/airflow/data"

# Default arguments for the DAG
default_args = {
    'owner': 'Neylson Crepalde',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['neylson.crepalde@a3data.com.br'],
    'email_on_failure': False,
    'email_on_retry': False
}

@dag(default_args=default_args, schedule_interval="*/3 * * * *", catchup=False, 
    tags=["example", "taskflow"], description="Example of a simple DAG")
def simple_dag():

    @task
    def generate_info_json(number):
        import random
        name = random.choice(["Neylson", "John", "Lucy", "Sue"])
        age = random.randint(18, 60)
        file_path = f"{base_path}/info{number}.json"
        with open(file_path, "w") as f:
            f.write(f"{{\"name\": \"{name}\", \"age\": {age}}}")
        return file_path

    
    @task
    def calculate_mean_age(path1, path2, path3):
        import json
        with open(path1, "r") as f:
            info1 = json.load(f)
        with open(path2, "r") as f:
            info2 = json.load(f)
        with open(path3, "r") as f:
            info3 = json.load(f)
        meanage = (info1["age"] + info2["age"] + info3["age"]) / 3
        with open(f"{base_path}/mean_age.json", "w") as f:
            f.write(f"{{\"mean_age\": {meanage}}}")
        return True


    @task
    def say_mean_age(b):
        if b:
            import json
            with open(f"{base_path}/mean_age.json", "r") as f:
                info = json.load(f)
            mean_age = info["mean_age"]
            print(f"The mean age is {mean_age}")
            return True
        else:
            print("The mean age could not be calculated")
            return False

    
    res1 = generate_info_json(1)
    res2 = generate_info_json(2)
    res3 = generate_info_json(3)
    meanage = calculate_mean_age(res1, res2, res3)
    say = say_mean_age(meanage)


execution = simple_dag()
