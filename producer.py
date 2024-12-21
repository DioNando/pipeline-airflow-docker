from kafka import KafkaProducer
import requests
import json

# Configuration du producer Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

# Fonction pour récupérer des données depuis une API publique
def fetch_data_from_api():
    api_url = 'https://jsonplaceholder.typicode.com/posts'  # Exemple d'API publique
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de l'appel API : {e}")
        return []

# Fonction pour publier les données dans un topic Kafka
def publish_to_kafka(data):
    for item in data:
        producer.send('scrap_topic', item)
        print(f"Message envoyé : {item}")

# Récupérer et publier les données
data = fetch_data_from_api()
if data:
    publish_to_kafka(data)
else:
    print("Aucune donnée à publier.")
