import os
import pymongo

def get_mongo_client():
    """
    Retorna un cliente de MongoDB usando la variable de entorno MONGO_URI.
    Si no está definida, usa 'mongodb://mongo:27017' como fallback.
    """
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    client = pymongo.MongoClient(mongo_uri)
    return client

