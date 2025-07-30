import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGODB_URI)
db = client["youtube_data"]  

st.set_page_config(page_title="YouTube Data Dashboard", layout="wide")
st.title("YouTube Data Dashboard")

video_collection = db.get_collection("videos")  # Changed from "video" to "videos"
video_data = list(video_collection.find())

if video_data:
    st.subheader("Video Information")
    video_df = pd.DataFrame(video_data)

    if '_id' in video_df.columns:
        video_df.drop(columns=['_id'], inplace=True)

    st.dataframe(video_df, use_container_width=True)
else:
    st.warning("No video data found in MongoDB.")
if "channel" in db.list_collection_names():
    channel_collection = db.get_collection("channel")
    channel_data = list(channel_collection.find())

    if channel_data:
        st.subheader("Channel Information")
        channel_df = pd.DataFrame(channel_data)
        if '_id' in channel_df.columns:
            channel_df.drop(columns=['_id'], inplace=True)
        st.dataframe(channel_df, use_container_width=True)
    else:
        st.warning("No channel data found in MongoDB.")
else:
    st.info("'channel' collection not found. Showing only video data.")
