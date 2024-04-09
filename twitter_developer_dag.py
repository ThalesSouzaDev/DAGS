from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import tweepy
import os

def create_folder_and_file():
    path = '/home/bigdata/Documents/airflow_env/twitter_data080424'
    os.makedirs(path, exist_ok=True)  # Cria a pasta se não existir
    open(os.path.join(path, 'dados_twitter.txt'), 'a').close()  # Cria o arquivo se não existir

def get_twitter_data():
    path = '/caminho/para/sua/pasta/dados_twitter.txt'
    api_key = Variable.get("twitter_api_key")
    api_key_secret = Variable.get("twitter_api_key_secret")
    access_token = Variable.get("twitter_access_token")
    access_token_secret = Variable.get("twitter_access_token_secret")
    
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    with open(path, 'a') as file:
        # Limita para os 100 primeiros tweets
        for tweet in api.home_timeline(count=1):
            file.write(f"{tweet.text}\n")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Opcional: Adicione 'execution_timeout' aqui se você quiser definir um limite de tempo global para todas as tarefas
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='A simple Twitter DAG',
    schedule_interval='@daily',
    catchup=False,
)

create_folder_and_file_task = PythonOperator(
    task_id='create_folder_and_file',
    python_callable=create_folder_and_file,
    dag=dag,
)

get_twitter_data_task = PythonOperator(
    task_id='get_twitter_data',
    python_callable=get_twitter_data,
    execution_timeout=timedelta(minutes=5),  # Define o limite de tempo para 30 minutos para esta tarefa específica
    dag=dag,
)

create_folder_and_file_task >> get_twitter_data_task
