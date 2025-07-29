import os
from dotenv import load_dotenv
from pymongo import MongoClient
from googleapiclient.discovery import build

load_dotenv()

def fetch_and_store_data():
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        print(" MONGODB_URI not found in .env file")
        return

    try:
        client = MongoClient(mongo_uri)
        db = client['youtube_data']
        collection = db['videos']
        print(" Connected to MongoDB")
    except Exception as e:
        print(f" MongoDB connection error: {e}")
        return

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print(" YOUTUBE_API_KEY not found in .env file")
        return

    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        print(" Connected to YouTube API")
    except Exception as e:
        print(f" YouTube API error: {e}")
        return

    channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'

    try:
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=5
        )
        response = request.execute()

        for item in response['items']:
            collection.insert_one(item)
            print(f" Inserted: {item['snippet']['title']}")
    except Exception as e:
        print(f" Error fetching/storing data: {e}")
