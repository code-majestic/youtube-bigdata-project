import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)
db = client["youtube_data"]
collection = db["videos"]

st.set_page_config(page_title="YouTube Big Data Project Dashboard", layout="wide")
st.title(" YouTube Big Data Project Dashboard")

data = list(collection.find())

if not data:
    st.warning("No data found in MongoDB. Please run scripts/main.py first.")
else:
    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)

    st.subheader(" Raw YouTube Data")
    st.dataframe(df)

    st.subheader(" Channel-wise Video Count")
    channel_count = df['channelTitle'].value_counts().reset_index()
    channel_count.columns = ['Channel', 'Video Count']
    st.bar_chart(channel_count.set_index("Channel"))

    st.subheader(" Most Viewed Videos")
    top_views = df[['title', 'viewCount']].sort_values(by='viewCount', ascending=False).head(10)
    st.table(top_views)

    st.subheader(" Like Count Distribution")
    st.line_chart(df[['likeCount']].dropna().head(50))
