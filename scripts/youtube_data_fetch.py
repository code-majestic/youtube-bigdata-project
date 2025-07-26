import os
from dotenv import load_dotenv
from pymongo import MongoClient
from googleapiclient.discovery import build

load_dotenv()

def fetch_and_store_data():
    load_dotenv()

    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        print("MONGODB_URI not found in .env file")
        return

    client = MongoClient(mongo_uri)
    db = client['youtube_data']
    collection = db['videos']

    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build('youtube', 'v3', developerKey=api_key)

    channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'

    request = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=5
    )
    response = request.execute()

    for item in response['items']:
        collection.insert_one(item)
        print(f"Inserted: {item['snippet']['title']}")