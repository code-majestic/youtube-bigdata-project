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
    st.subheader("Video Titles:")
    st.write(df['snippet'].apply(lambda x: x['title']))

    st.subheader("Published At Dates:")
    st.bar_chart(df['snippet'].apply(lambda x: x['publishedAt']))
else:
    st.write("No data available in MongoDB. Please fetch first.")