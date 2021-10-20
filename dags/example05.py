import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

data_path = '/usr/local/airflow/data/3.DADOS/'
datafile = data_path + 'microdados_enade_2019.txt'

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    description="Processing of Brazilian Higher Education Assessment Dataset",
    default_args=default_args, 
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=['enade', 'taskflow', 'realworld'],
    max_active_runs=1,
)
def higher_education_assessment():
   
    get_data = BashOperator(
        task_id="get-data",
        bash_command='curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    )

    @task
    def unzip_file():
        import zipfile
        with zipfile.ZipFile("/usr/local/airflow/data/microdados_enade_2019.zip", 'r') as zipped:
            zipped.extractall("/usr/local/airflow/data")


    @task
    def apply_filters():
        import pandas as pd
        cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
                'QE_I01','QE_I02','QE_I04','QE_I05','QE_I08']
        enade = pd.read_csv(datafile, sep=';', decimal=',', usecols=cols)
        enade['NT_GER'] = pd.to_numeric(enade['NT_GER'], errors='coerce')
        enade = enade.loc[
            (enade.NU_IDADE > 20) &
            (enade.NU_IDADE < 40) &
            (enade.NT_GER > 0)
        ]
        enade.to_csv(data_path + 'enade_filtrado.csv', index=False)

    

    @task
    def build_mean_centered_age():
        import pandas as pd
        idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
        idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
        idade[['idadecent']].to_csv(data_path + "idadecent.csv", index=False)


    @task
    def build_squared_centered_age():
        import pandas as pd
        idadecent = pd.read_csv(data_path + "idadecent.csv")
        idadecent['idade2'] = idadecent.idadecent ** 2
        idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)


    @task
    def build_marital_status():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
        filtro['estcivil'] = filtro.QE_I01.replace({
            'A': 'Solteiro',
            'B': 'Casado',
            'C': 'Separado',
            'D': 'Viúvo',
            'E': 'Outro'
        })
        filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)


    @task
    def build_race():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
        filtro['cor'] = filtro.QE_I02.replace({
            'A': 'Branca',
            'B': 'Preta',
            'C': 'Amarela',
            'D': 'Parda',
            'E': 'Indígena',
            'F': "",
            ' ': ""
        })
        filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)


    @task
    def build_fathers_education():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I04'])
        filtro['escopai'] = filtro.QE_I04.replace({
            'A': 0,
            'B': 1,
            'C': 2,
            'D': 3,
            'E': 4,
            'F': 5
        })
        filtro[['escopai']].to_csv(data_path + 'escopai.csv', index=False)


    @task
    def build_mothers_education():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I05'])
        filtro['escomae'] = filtro.QE_I05.replace({
                'A': 0,
                'B': 1,
                'C': 2,
                'D': 3,
                'E': 4,
                'F': 5
            })
        filtro[['escomae']].to_csv(data_path + 'escomae.csv', index=False)

    
    @task
    def build_income():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I08'])
        filtro['renda'] = filtro.QE_I08.replace({
            'A': 0,
            'B': 1,
            'C': 2,
            'D': 3,
            'E': 4,
            'F': 5,
            'G': 6
        })
        filtro[['renda']].to_csv(data_path + 'renda.csv', index=False)

    
    @task
    def join_data():
        import pandas as pd
        filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
        idadecent = pd.read_csv(data_path + 'idadecent.csv')
        idadequadrado = pd.read_csv(data_path + 'idadequadrado.csv')
        estcivil = pd.read_csv(data_path + 'estcivil.csv')
        cor = pd.read_csv(data_path + 'cor.csv')
        escopai = pd.read_csv(data_path + 'escopai.csv')
        escomae = pd.read_csv(data_path + 'escomae.csv')
        renda = pd.read_csv(data_path + 'renda.csv')
        
        final = pd.concat([filtro, idadecent, idadequadrado, estcivil, cor,
                            escopai, escomae, renda], axis=1)
        final.to_csv(data_path + 'enade_tratado.csv', index=False)
        print(final)

    

    t_unzip_file = unzip_file()
    t_apply_filters = apply_filters()
    t_build_mean_centered_age = build_mean_centered_age()
    t_build_squared_centered_age = build_squared_centered_age()
    t_build_marital_status = build_marital_status()
    t_build_race = build_race()
    t_build_fathers_education = build_fathers_education()
    t_build_mothers_education = build_mothers_education()
    t_build_income = build_income()
    t_join_data = join_data()

    get_data >> t_unzip_file >> t_apply_filters >> [
        t_build_mean_centered_age, t_build_marital_status, t_build_race, 
        t_build_fathers_education, t_build_mothers_education, t_build_income
    ]
    
    t_build_mean_centered_age >> t_build_squared_centered_age
    
    [
        t_build_mean_centered_age, t_build_marital_status, t_build_race, 
        t_build_fathers_education, t_build_mothers_education, t_build_income,
        t_build_squared_centered_age
    ] >> t_join_data

execution = higher_education_assessment()
