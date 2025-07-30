import pymongo
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
client = pymongo.MongoClient(MONGODB_URI)
db = client["youtube_data"]
collection = db["videos"]

def get_channel_data():
    data = list(collection.find({}, {'_id': 0}))
    df = pd.DataFrame(data)
    
    if 'snippet' in df.columns:
        snippet_df = pd.json_normalize(df['snippet'])
        df = pd.concat([df.drop(columns=['snippet']), snippet_df], axis=1)

    return df