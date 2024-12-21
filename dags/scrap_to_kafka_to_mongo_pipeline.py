from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json
import requests
from bs4 import BeautifulSoup

# Configuration
API_URI = "https://pantheon.world/explore/rankings?show=people&years=-3501,2023"
KAFKA_TOPIC = "scrap_topic"
KAFKA_BROKER = "kafka:9092"
MONGO_URI = "mongodb://root:example@mongodb:27017/"
MONGO_DB = "airflow_db"
MONGO_COLLECTION = "processed_data"
EMAIL_RECIPIENT = "your_email@example.com"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# DAG Definition
with DAG(
    "scrap_to_kafka_to_mongo_pipeline",
    default_args=default_args,
    description="Pipeline to fetch data, process via Kafka, and store in MongoDB",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Fetch data from API and push to Kafka
    def scrape_and_send_to_kafka():
        try:
            # Effectuer une requête GET
            response = requests.get(API_URI)

            # Vérifier si la requête a réussi
            if response.status_code == 200:
                # Parser le contenu HTML de la page
                soup = BeautifulSoup(response.text, 'html.parser')

                # Initialiser Kafka Producer
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )

                # Récupérer tous les titres H1
                h1_titles = soup.find_all('h1')
                for index, title in enumerate(h1_titles, start=1):
                    data = {"type": "title", "index": index, "content": title.text.strip()}
                    producer.send(KAFKA_TOPIC, value=data)
                    print(f"Envoyé au Kafka: {data}")

                # Récupérer tous les liens
                links = soup.find_all('a', href=True)
                for index, link in enumerate(links[:10], start=1):  # Limité à 10 pour cet exemple
                    data = {"type": "link", "index": index, "content": link['href']}
                    producer.send(KAFKA_TOPIC, value=data)
                    print(f"Envoyé au Kafka: {data}")

                # Finaliser l'envoi
                producer.flush()
                producer.close()
                print("Tous les messages ont été envoyés au Kafka.")

            else:
                print(f"Échec de la requête, code de statut : {response.status_code}")

        except Exception as e:
            print(f"Une erreur est survenue : {e}")


    fetch_to_kafka = PythonOperator(
        task_id="scrape_and_send_to_kafka",
        python_callable=scrape_and_send_to_kafka,
    )

    def consume_kafka_and_store_to_mongo():
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",  # Commence à consommer depuis le début
            enable_auto_commit=True,  # Active l'auto-commit des offsets
        )
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        messages_consumed = 0
        max_messages = 100

        # Poll for messages
        while messages_consumed < max_messages:
            records = consumer.poll(timeout_ms=5000)  # Attendre les messages pendant 5 secondes
            for topic_partition, messages in records.items():
                for message in messages:
                    # Process message and store it in MongoDB
                    processed_data = {"title": message.value["title"].upper()}  # Exemple de traitement
                    collection.insert_one(processed_data)
                    messages_consumed += 1
                    if messages_consumed >= max_messages:
                        break

        consumer.close()
        client.close()

    consume_kafka = PythonOperator(
        task_id="consume_kafka_and_store_to_mongo",
        python_callable=consume_kafka_and_store_to_mongo,
    )

    # Task 3: Send email notification on success
    notify_success = EmailOperator(
        task_id="send_success_email",
        to=EMAIL_RECIPIENT,
        subject="Airflow Pipeline Success",
        html_content="<h3>The pipeline executed successfully!</h3>",
    )

    # Task Dependencies
    fetch_to_kafka >> consume_kafka >> notify_success
