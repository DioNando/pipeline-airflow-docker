from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Configuration MongoDB
# mongo_client = MongoClient('mongodb://root:example@mongodb:27017/?authSource=admin')  # Connexion à MongoDB
mongo_client = MongoClient('mongodb://root:example@localhost:27017/?authSource=admin')  # Connexion à MongoDB
db = mongo_client['bigdata_project']  # Nom de la base de données
collection = db['api_data']  # Nom de la collection

# Configuration Kafka Consumer
consumer = KafkaConsumer(
    'scrap_topic',  # Nom du topic Kafka
    bootstrap_servers='localhost:9092',  # Adresse du serveur Kafka
    auto_offset_reset='earliest',  # Lire les messages depuis le début si c'est la première fois
    enable_auto_commit=True,  # Confirme automatiquement les messages consommés
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialisation des données JSON
)

print("Consumer démarré. En attente des messages...")

# Lecture et insertion dans MongoDB
for message in consumer:
    data = message.value  # Données extraites du message Kafka
    collection.insert_one(data)  # Insertion dans MongoDB
    print(f"Message inséré dans MongoDB : {data}")
