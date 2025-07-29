import pymongo
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI)
db = client["youtube_data"]
collection = db["channels"]

def get_channel_data():
    data = list(collection.find({}, {'_id': 0}))
    return pd.DataFrame(data)