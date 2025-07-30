import os
from dotenv import load_dotenv
from pymongo import MongoClient
from googleapiclient.discovery import build

load_dotenv()
def fetch_and_store_data():
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        print("MONGODB_URI not found in .env file")
        return

    try:
        client = MongoClient(mongodb_uri)
        db = client['youtube_data']
        videos_collection = db['videos']
        channel_collection = db['channel']  
        print("Connected to MongoDB")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("YOUTUBE_API_KEY not found in .env file")
        return
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        print("Connected to YouTube API")
    except Exception as e:
        print(f"YouTube API error: {e}")
        return

    channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'

    try:

        channel_response = youtube.channels().list(
            part='snippet,statistics',
            id=channel_id
        ).execute()

        if channel_response['items']:
            channel_data = channel_response['items'][0]
            
            channel_collection.insert_one(channel_data)
            print(f"Inserted channel: {channel_data['snippet']['title']}")

        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=5
        )
        response = request.execute()

        for item in response['items']:
            videos_collection.insert_one(item)
            print(f"Inserted video: {item['snippet']['title']}")

    except Exception as e:
        print(f"Error fetching/storing data: {e}")