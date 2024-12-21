from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

# Fonction d'extraction
def extract_data():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    with open("/tmp/data.csv", "wb") as file:
        file.write(response.content)
    print("Fichier téléchargé et sauvegardé sous /tmp/data.csv")

# Fonction de lecture
def read_data():
    data = pd.read_csv("/tmp/data.csv")
    print("Contenu du fichier :")
    print(data.head())  # Affiche les 5 premières lignes

# Définition du DAG
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_extraction",
    default_args=default_args,
    description="DAG pour extraire et lire un fichier CSV",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # Tâche d'extraction
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # Tâche de lecture
    read_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    # Définition de la séquence
    extract_task >> read_task
