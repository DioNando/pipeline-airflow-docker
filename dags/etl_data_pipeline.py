from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import sqlite3

# Fonction d'extraction
def extract_data():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    with open("/tmp/data.csv", "wb") as file:
        file.write(response.content)
    print("Fichier téléchargé et sauvegardé sous /tmp/data.csv")

# Fonction de transformation
def transform_data():
    data = pd.read_csv("/tmp/data.csv")
    print("Données brutes :")
    print(data.head())
    
    # Suppression des lignes avec des valeurs manquantes
    cleaned_data = data.dropna()
    cleaned_data.to_csv("/tmp/cleaned_data.csv", index=False)
    print("Données nettoyées et sauvegardées sous /tmp/cleaned_data.csv")

# Fonction de chargement
def load_data():
    cleaned_data = pd.read_csv("/tmp/cleaned_data.csv")
    
    # Connexion à SQLite
    conn = sqlite3.connect("/tmp/airflow_etl.db")
    cleaned_data.to_sql("cleaned_data", conn, if_exists="replace", index=False)
    print("Données chargées dans la base SQLite /tmp/airflow_etl.db, table 'cleaned_data'")
    conn.close()

# Configuration du DAG
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_data_pipeline",
    default_args=default_args,
    description="DAG ETL pour extraire, transformer et charger des données",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # Tâche d'extraction
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # Tâche de transformation
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Tâche de chargement
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Définition de la séquence des tâches
    extract_task >> transform_task >> load_task
