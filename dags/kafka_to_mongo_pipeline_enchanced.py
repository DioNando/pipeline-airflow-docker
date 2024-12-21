import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

import json
import requests
import logging
from typing import List, Dict

# Logging Configuration
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration Constants
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'bigdata_topic')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
MONGO_DB = os.getenv('MONGO_DB', 'airflow_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'processed_data')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT', 'admin@example.com')

def validate_data(data: Dict) -> bool:
    """
    Validate incoming data before processing
    
    Args:
        data (Dict): Input data dictionary
    
    Returns:
        bool: Whether data is valid
    """
    required_keys = ['id', 'title', 'body']
    return all(key in data for key in required_keys)

def fetch_data_with_retry(url: str, max_retries: int = 3) -> List[Dict]:
    """
    Fetch data from API with retry mechanism
    
    Args:
        url (str): API endpoint URL
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        List[Dict]: Fetched data
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Fetch attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise AirflowException(f"Failed to fetch data after {max_retries} attempts")

def enrich_data(data: Dict) -> Dict:
    """
    Enrich data with additional metadata
    
    Args:
        data (Dict): Original data
    
    Returns:
        Dict: Enriched data
    """
    return {
        **data,
        'processed_timestamp': datetime.now().isoformat(),
        'source': 'jsonplaceholder_api',
        'data_status': 'raw'
    }

def fetch_data_and_push_to_kafka(**context):
    """
    Task to fetch data from API and push to Kafka
    """
    try:
        # Fetch data with retry mechanism
        data = fetch_data_with_retry("https://jsonplaceholder.typicode.com/posts")
        
        # Filter and validate data
        valid_data = [enrich_data(item) for item in data if validate_data(item)]
        
        # Push to Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for item in valid_data:
            producer.send(KAFKA_TOPIC, item)
        
        producer.flush()
        producer.close()
        
        # Push number of processed items to XCom for tracking
        context['ti'].xcom_push(key='processed_items', value=len(valid_data))
        
        logger.info(f"Successfully pushed {len(valid_data)} items to Kafka")
    
    except Exception as e:
        logger.error(f"Error in fetch_data_and_push_to_kafka: {e}")
        raise

def consume_kafka_and_store_to_mongo(**context):
    """
    Task to consume Kafka messages and store in MongoDB
    """
    try:
        # Retrieve connection details from Airflow connections
        mongo_conn = BaseHook.get_connection('mongo_default')
        
        # Construct MongoDB URI
        mongo_uri = f"mongodb://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}:{mongo_conn.port}/"
        
        client = MongoClient(mongo_uri)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='airflow_consumer_group'
        )
        
        processed_items = 0
        for message in consumer:
            try:
                data = message.value
                result = collection.update_one(
                    {'id': data['id']},  # Upsert based on unique identifier
                    {'$set': data},
                    upsert=True
                )
                processed_items += 1
                
                # Stop after processing a reasonable number of messages
                if processed_items >= 50:
                    break
            
            except PyMongoError as mongo_err:
                logger.error(f"MongoDB insertion error: {mongo_err}")
        
        consumer.close()
        client.close()
        
        # Push processed items count to XCom
        context['ti'].xcom_push(key='mongo_processed_items', value=processed_items)
        
        logger.info(f"Successfully processed {processed_items} items in MongoDB")
    
    except Exception as e:
        logger.error(f"Error in consume_kafka_and_store_to_mongo: {e}")
        raise

def generate_email_content(**context):
    """
    Generate dynamic email content based on task results
    """
    processed_items = context['ti'].xcom_pull(task_ids='fetch_data_and_push_to_kafka', key='processed_items')
    mongo_items = context['ti'].xcom_pull(task_ids='consume_kafka_and_store_to_mongo', key='mongo_processed_items')
    
    return f"""
    <html>
        <body>
            <h2>Airflow Pipeline Execution Report</h2>
            <p>Processed Items: {processed_items}</p>
            <p>MongoDB Stored Items: {mongo_items}</p>
            <p>Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </body>
    </html>
    """

# Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# DAG Definition
with DAG(
    'enhanced_kafka_mongo_pipeline',
    default_args=default_args,
    description='Advanced Data Pipeline with Kafka and MongoDB',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    # Tasks
    fetch_to_kafka = PythonOperator(
        task_id='fetch_data_and_push_to_kafka',
        python_callable=fetch_data_and_push_to_kafka,
        provide_context=True
    )

    consume_kafka = PythonOperator(
        task_id='consume_kafka_and_store_to_mongo',
        python_callable=consume_kafka_and_store_to_mongo,
        provide_context=True
    )

    notify_success = EmailOperator(
        task_id='send_success_email',
        to=EMAIL_RECIPIENT,
        subject='Airflow Pipeline Execution Report',
        html_content="{{ task_instance.xcom_pull(task_ids='generate_email_content') }}",
    )

    generate_email = PythonOperator(
        task_id='generate_email_content',
        python_callable=generate_email_content,
        provide_context=True
    )

    # Task Dependencies
    fetch_to_kafka >> consume_kafka >> generate_email >> notify_success