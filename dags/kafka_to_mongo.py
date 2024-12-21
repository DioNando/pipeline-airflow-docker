from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

def produce_to_kafka():
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    producer.send('my_topic', b'This is a message from Airflow!')
    producer.close()

def consume_from_kafka_and_store_in_mongo():
    consumer = KafkaConsumer('my_topic', bootstrap_servers='kafka:9092', auto_offset_reset='earliest')
    client = MongoClient("mongodb://root:example@mongodb:27017/?authSource=admin")
    db = client["my_database"]
    collection = db["my_collection"]
    for message in consumer:
        collection.insert_one({"message": message.value.decode('utf-8')})
    consumer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
}

with DAG('kafka_to_mongo', default_args=default_args, schedule_interval=None) as dag:
    producer_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka
    )

    consumer_task = PythonOperator(
        task_id='consume_from_kafka_and_store_in_mongo',
        python_callable=consume_from_kafka_and_store_in_mongo
    )

    producer_task >> consumer_task
