from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def get_mongodb_client():
    mongodb_uri = os.getenv("MONGODB_URI")
    return MongoClient(mongodb_uri)

def fetch_videos_from_db():
    client = get_mongodb_client()
    db = client['youtube_data']  
    collection = db['videos']
    return list(collection.find())