from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
import requests
import pandas as pd
from pymongo import MongoClient

# Fonction d'extraction de la première source
def extract_data_source_1():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"
    response = requests.get(url)
    with open("/tmp/source1.csv", "wb") as file:
        file.write(response.content)
    print("Source 1 téléchargée.")

# Fonction d'extraction de la deuxième source
def extract_data_source_2():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/grades.csv"
    response = requests.get(url)
    with open("/tmp/source2.csv", "wb") as file:
        file.write(response.content)
    print("Source 2 téléchargée.")

# Fonction de combinaison des deux sources
def combine_sources():
    df1 = pd.read_csv("/tmp/source1.csv")
    df2 = pd.read_csv("/tmp/source2.csv")
    combined_df = pd.concat([df1, df2], ignore_index=True)
    combined_df.to_json("/tmp/combined_data.json", orient="records")
    print("Données combinées et sauvegardées sous /tmp/combined_data.json")

# Fonction de transformation
def transform_data():
    data = pd.read_json("/tmp/combined_data.json")
    print("Données brutes combinées :")
    print(data.head())
    cleaned_data = data.dropna(how='all')
    cleaned_data.to_json("/tmp/cleaned_data.json", orient="records")
    print("Données nettoyées et sauvegardées sous /tmp/cleaned_data.json")
    return {"nb_lignes": len(cleaned_data)}

# Fonction de chargement dans MongoDB
def load_data():
    client = MongoClient("mongodb://root:example@mongodb:27017/?authSource=admin")
    db = client["airflow_etl_db"]
    collection = db["cleaned_data"]
    with open("/tmp/cleaned_data.json", "r") as file:
        data = pd.read_json(file)
    collection.insert_many(data.to_dict("records"))
    print("Données chargées dans MongoDB.")
    client.close()

# Fonction de décision de branchement
def decide_branch(**kwargs):
    nb_lignes = kwargs['ti'].xcom_pull(task_ids='transform_data')['nb_lignes']
    if nb_lignes > 10:
        return "load_data"
    else:
        return "skip_load"

# Configuration du DAG
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_data_pipeline_mongodb_complex",
    default_args=default_args,
    description="DAG ETL complexe pour extraire, transformer et charger des données dans MongoDB",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # Tâches d'extraction
    extract_task_1 = PythonOperator(
        task_id="extract_data_source_1",
        python_callable=extract_data_source_1,
    )
    extract_task_2 = PythonOperator(
        task_id="extract_data_source_2",
        python_callable=extract_data_source_2,
    )

    # Tâche de capteur pour attendre un fichier
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/source1.csv",
        poke_interval=10,
        timeout=600,
    )

    # Tâche de combinaison
    combine_task = PythonOperator(
        task_id="combine_sources",
        python_callable=combine_sources,
    )

    # Tâche de transformation
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Tâche de branchement
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=decide_branch,
        provide_context=True,
    )

    # Tâche de chargement
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Tâche d'arrêt si les données sont insuffisantes
    skip_load_task = EmptyOperator(task_id="skip_load")

    # Tâche de notification par e-mail
    email_task = EmailOperator(
        task_id="send_email_on_success",
        to="bigdata@ismagi.net",
        subject="DAG ETL Success",
        html_content="Toutes les tâches du DAG ETL ont été exécutées avec succès.",
        trigger_rule="all_success",
    )

    # Définition des dépendances
    [extract_task_1, extract_task_2] >> wait_for_file >> combine_task >> transform_task >> branch_task
    branch_task >> [load_task, skip_load_task]
    load_task >> email_task
