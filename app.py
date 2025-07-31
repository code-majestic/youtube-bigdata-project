import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import plotly.express as px
from datetime import datetime

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGODB_URI)
db = client["youtube_data"]  

st.set_page_config(page_title="YouTube Analytics Dashboard", layout="wide")
st.title("🎥 YouTube Analytics Dashboard")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a page", ["Overview", "Video Analytics", "Channel Stats", "Raw Data"])

if page == "Overview":
    st.header("📊 Channel Overview")
    
    # Channel Information
    if "channel" in db.list_collection_names():
        channel_collection = db.get_collection("channel")
        channel_data = list(channel_collection.find())
        
        if channel_data:
            channel = channel_data[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                subscribers = channel.get('statistics', {}).get('subscriberCount', 'N/A')
                st.metric("Subscribers", f"{int(subscribers):,}" if subscribers != 'N/A' else 'N/A')
            
            with col2:
                total_views = channel.get('statistics', {}).get('viewCount', 'N/A')
                st.metric("Total Views", f"{int(total_views):,}" if total_views != 'N/A' else 'N/A')
            
            with col3:
                video_count = channel.get('statistics', {}).get('videoCount', 'N/A')
                st.metric("Total Videos", f"{int(video_count):,}" if video_count != 'N/A' else 'N/A')
            
            with col4:
                if 'snippet' in channel and 'publishedAt' in channel['snippet']:
                    created_date = datetime.fromisoformat(channel['snippet']['publishedAt'].replace('Z', '+00:00'))
                    st.metric("Channel Age", f"{(datetime.now() - created_date.replace(tzinfo=None)).days} days")
    
    # Video Statistics Summary
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        st.header("📈 Video Performance Summary")
        
        df = pd.DataFrame(video_data)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_views = df['view_count'].mean() if 'view_count' in df.columns else 0
            st.metric("Average Views", f"{int(avg_views):,}")
        
        with col2:
            avg_engagement = df['engagement_rate'].mean() if 'engagement_rate' in df.columns else 0
            st.metric("Avg Engagement Rate", f"{avg_engagement:.2f}%")
        
        with col3:
            avg_duration = df['duration_seconds'].mean() if 'duration_seconds' in df.columns else 0
            st.metric("Average Duration", f"{int(avg_duration//60)}:{int(avg_duration%60):02d}")

elif page == "Video Analytics":
    st.header("🎬 Video Analytics")
    
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        df = pd.DataFrame(video_data)
        
        if 'view_count' in df.columns and 'snippet' in df.columns:
            # Top 10 videos by views
            st.subheader("Top 10 Most Viewed Videos")
            
            # Extract title from snippet
            df['title'] = df['snippet'].apply(lambda x: x.get('title', 'No Title') if isinstance(x, dict) else 'No Title')
            
            top_videos = df.nlargest(10, 'view_count')[['title', 'view_count', 'like_count', 'comment_count']]
            st.dataframe(top_videos, use_container_width=True)
            
            # Views distribution
            if len(df) > 0:
                st.subheader("Views Distribution")
                fig = px.histogram(df, x='view_count', nbins=20, title="Distribution of Video Views")
                st.plotly_chart(fig, use_container_width=True)
                
                # Engagement vs Views scatter plot
                if 'engagement_rate' in df.columns:
                    st.subheader("Engagement Rate vs Views")
                    fig2 = px.scatter(df, x='view_count', y='engagement_rate', 
                                    hover_data=['title'], title="Engagement Rate vs Views")
                    st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No video data found.")

elif page == "Channel Stats":
    st.header("📺 Channel Statistics")
    
    if "channel" in db.list_collection_names():
        channel_collection = db.get_collection("channel")
        channel_data = list(channel_collection.find())
        
        if channel_data:
            channel = channel_data[0]
            
            st.subheader("Channel Information")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Channel Name:**", channel.get('snippet', {}).get('title', 'N/A'))
                st.write("**Description:**", channel.get('snippet', {}).get('description', 'N/A')[:200] + "...")
                st.write("**Country:**", channel.get('snippet', {}).get('country', 'N/A'))
            
            with col2:
                stats = channel.get('statistics', {})
                st.write("**Subscribers:**", f"{int(stats.get('subscriberCount', 0)):,}")
                st.write("**Total Views:**", f"{int(stats.get('viewCount', 0)):,}")
                st.write("**Videos:**", f"{int(stats.get('videoCount', 0)):,}")
                
                if 'snippet' in channel and 'publishedAt' in channel['snippet']:
                    st.write("**Created:**", channel['snippet']['publishedAt'][:10])
    
    # Video duration analysis
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        df = pd.DataFrame(video_data)
        
        if 'duration_seconds' in df.columns:
            st.subheader("Video Duration Analysis")
            
            # Duration categories
            def categorize_duration(seconds):
                if seconds < 300:  # 5 minutes
                    return "Short (< 5 min)"
                elif seconds < 1200:  # 20 minutes
                    return "Medium (5-20 min)"
                else:
                    return "Long (> 20 min)"
            
            df['duration_category'] = df['duration_seconds'].apply(categorize_duration)
            
            duration_counts = df['duration_category'].value_counts()
            fig = px.pie(values=duration_counts.values, names=duration_counts.index, 
                        title="Video Duration Distribution")
            st.plotly_chart(fig, use_container_width=True)

elif page == "Raw Data":
    st.header("📋 Raw Data")
    
    tab1, tab2 = st.tabs(["Video Data", "Channel Data"])
    
    with tab1:
        video_collection = db.get_collection("videos")
        video_data = list(video_collection.find())
        
        if video_data:
            df = pd.DataFrame(video_data)
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
            
            st.subheader(f"Video Data ({len(df)} videos)")
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download Video Data as CSV",
                data=csv,
                file_name=f"youtube_videos_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No video data found.")
    
    with tab2:
        if "channel" in db.list_collection_names():
            channel_collection = db.get_collection("channel")
            channel_data = list(channel_collection.find())
            
            if channel_data:
                channel_df = pd.DataFrame(channel_data)
                if '_id' in channel_df.columns:
                    channel_df.drop(columns=['_id'], inplace=True)
                
                st.subheader("Channel Data")
                st.dataframe(channel_df, use_container_width=True)
            else:
                st.warning("No channel data found.")
        else:
            st.info("Channel collection not found.")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))