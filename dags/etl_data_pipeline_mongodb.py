from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from pymongo import MongoClient

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
    cleaned_data.to_json("/tmp/cleaned_data.json", orient="records")
    print("Données nettoyées et sauvegardées sous /tmp/cleaned_data.json")

# Fonction de chargement dans MongoDB
def load_data():
    client = MongoClient("mongodb://root:example@mongodb:27017/?authSource=admin")  
    db = client["airflow_etl_db"]  # Base de données
    collection = db["cleaned_data"]  # Collection (table)

    # Lecture des données nettoyées
    with open("/tmp/cleaned_data.json", "r") as file:
        data = pd.read_json(file)
    
    # Insertion dans MongoDB
    collection.insert_many(data.to_dict("records"))
    print("Données chargées dans MongoDB, collection 'cleaned_data'")
    client.close()

# Configuration du DAG
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_data_pipeline_mongodb",
    default_args=default_args,
    description="DAG ETL pour extraire, transformer et charger des données dans MongoDB",
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
