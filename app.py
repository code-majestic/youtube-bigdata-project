import streamlit as st
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
mongo_uri = os.getenv("MONGODB_URI")

client = MongoClient(mongo_uri)
db = client["youtube_data"]
collection = db["videos"]

data = list(collection.find())
df = pd.DataFrame(data)

st.title("📊 YouTube Video Dashboard")

if not df.empty:
    df['video_title'] = df['snippet'].apply(lambda x: x.get('title'))
    df['channel_title'] = df['snippet'].apply(lambda x: x.get('channelTitle'))
    df['published_date'] = df['snippet'].apply(lambda x: x.get('publishedAt'))
    df['video_id'] = df['id'].apply(lambda x: x.get('videoId'))

    st.subheader("🎥 Video Details")
    st.dataframe(df[['video_title', 'channel_title', 'published_date', 'video_id']])

    st.subheader("🗓️ Videos Published Over Time")
    df['published_date'] = pd.to_datetime(df['published_date'])
    published_count = df.groupby(df['published_date'].dt.date).size()
    st.line_chart(published_count)

else:
    st.warning("No data available in MongoDB. Please fetch first.")
    
import streamlit as st
from scripts.data_visualizer import get_channel_data

st.set_page_config(page_title="YouTube Data Explorer", layout="wide")

st.title("📊 YouTube Channel Data Explorer")

st.write("Below is the data fetched from MongoDB:")

df = get_channel_data()

if not df.empty:
    st.dataframe(df)
else:
    st.warning("No data found in MongoDB.")