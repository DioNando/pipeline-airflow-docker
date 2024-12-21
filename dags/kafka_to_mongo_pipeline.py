from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json
import requests

# Configuration
KAFKA_TOPIC = "bigdata_topic"
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
    "kafka_to_mongo_pipeline",
    default_args=default_args,
    description="Pipeline to fetch data, process via Kafka, and store in MongoDB",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Fetch data from API and push to Kafka
    def fetch_data_and_push_to_kafka():
        response = requests.get("https://jsonplaceholder.typicode.com/posts")  # Replace with your API URL
        if response.status_code == 200:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            data = response.json()
            for item in data:
                producer.send(KAFKA_TOPIC, item)
            producer.flush()
            producer.close()
        else:
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

    fetch_to_kafka = PythonOperator(
        task_id="fetch_data_and_push_to_kafka",
        python_callable=fetch_data_and_push_to_kafka,
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
