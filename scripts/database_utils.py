from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI")
    return MongoClient(mongo_uri)

def fetch_videos_from_db():
    client = get_mongo_client()
    db = client['youtube_data']  # same as used during insert
    collection = db['videos']
    return list(collection.find())