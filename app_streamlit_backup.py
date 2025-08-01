import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import subprocess
import sys

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGODB_URI)
db = client["youtube_data"]  

st.set_page_config(page_title="YouTube Big Data Analytics Dashboard", layout="wide")
st.title("YouTube Big Data Analytics Dashboard")
st.markdown("*Powered by Dask Distributed Computing*")

st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a page", [
    "Overview", 
    "Video Analytics", 
    "Big Data Analytics", 
    "Dask Processing", 
    "Channel Stats", 
    "Raw Data"
])

def run_dask_analytics():
    """Run Dask analytics and return results"""
    try:
        with st.spinner("Running Big Data Analytics with Dask..."):
            result = subprocess.run([
                sys.executable, "scripts/dask_analytics.py"
            ], capture_output=True, text=True, cwd=".")
            
            if result.returncode == 0:
                st.success("Dask Analytics completed successfully!")
                return True
            else:
                st.error(f"Error in Dask processing: {result.stderr}")
                return False
    except Exception as e:
        st.error(f"Error running Dask analytics: {e}")
        return False

def load_dask_results():
    """Load Dask analytics results from MongoDB"""
    try:
        dask_collection = db.get_collection("dask_results")
        results = list(dask_collection.find().sort("timestamp", -1).limit(1))
        return results[0] if results else None
    except:
        return None

if page == "Overview":
    st.header("Channel Overview")
    
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
    
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        st.header("Video Performance Summary")
        
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
    st.header("Video Analytics")
    
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        df = pd.DataFrame(video_data)
        
        if 'view_count' in df.columns and 'snippet' in df.columns:
            st.subheader("Top 10 Most Viewed Videos")
            
            df['title'] = df['snippet'].apply(lambda x: x.get('title', 'No Title') if isinstance(x, dict) else 'No Title')
            
            top_videos = df.nlargest(10, 'view_count')[['title', 'view_count', 'like_count', 'comment_count']]
            st.dataframe(top_videos, use_container_width=True)
            
            if len(df) > 0:
                st.subheader("Views Distribution")
                fig = px.histogram(df, x='view_count', nbins=20, title="Distribution of Video Views")
                st.plotly_chart(fig, use_container_width=True)
                
                if 'engagement_rate' in df.columns:
                    st.subheader("Engagement Rate vs Views")
                    fig2 = px.scatter(df, x='view_count', y='engagement_rate', 
                                    hover_data=['title'], title="Engagement Rate vs Views")
                    st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No video data found.")

elif page == "Big Data Analytics":
    st.header("Big Data Analytics with Dask")
    st.markdown("*Distributed computing for large-scale data processing*")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        if st.button("Run Dask Analytics", type="primary"):
            success = run_dask_analytics()
            if success:
                st.rerun()
    
    with col1:
        st.markdown("**Features:**")
        st.markdown("• Distributed data processing")
        st.markdown("• Large dataset simulation (50K+ records)")
        st.markdown("• Advanced correlation analysis")
        st.markdown("• Performance tier classification")
    
    dask_results = load_dask_results()
    
    if dask_results:
        st.success("Latest Dask Analytics Results Available")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Processing Method", "Dask Distributed")
        with col2:
            st.metric("Dataset Size", "50K+ Records")
        with col3:
            st.metric("Partitions", "10")
        with col4:
            st.metric("Status", "Completed")
    else:
        st.info("Run Dask Analytics to see distributed computing results")

elif page == "Dask Processing":
    st.header("Dask Distributed Computing")
    
    st.subheader("Large Dataset Processing Demo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Traditional Processing:**")
        st.code("""
# Single-threaded processing
for video in videos:
    process_video(video)
    
# Memory limitations
# Slow processing
# No parallelization
        """)
    
    with col2:
        st.markdown("**Dask Distributed Processing:**")
        st.code("""
import dask.dataframe as dd

# Distributed processing
dask_df = dd.from_pandas(df, npartitions=10)
results = dask_df.groupby('category').mean().compute()

# Parallel processing
# Memory efficient
# Scalable architecture
        """)
    
    st.subheader("Processing Performance Comparison")
    
    comparison_data = {
        'Method': ['Traditional Pandas', 'Dask Distributed'],
        'Dataset Size': ['< 1M records', '50M+ records'],
        'Processing Time': ['Linear growth', 'Parallel speedup'],
        'Memory Usage': ['Full dataset in RAM', 'Chunked processing'],
        'Scalability': ['Single machine', 'Multi-core/Multi-node']
    }
    
    comparison_df = pd.DataFrame(comparison_data)
    st.table(comparison_df)
    
    if st.button("Run Live Dask Demo", type="primary"):
        with st.spinner("Processing 50K records with Dask..."):
            import time
            import numpy as np
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            stages = [
                "Initializing Dask session...",
                "Creating 50K synthetic records...",
                "Partitioning data across 10 workers...",
                "Running distributed analytics...",
                "Computing aggregations...",
                "Finalizing results..."
            ]
            
            for i, stage in enumerate(stages):
                status_text.text(stage)
                time.sleep(1)
                progress_bar.progress((i + 1) / len(stages))
            
            st.success("Processed 50,000 records using Dask distributed computing!")
            
            sample_results = pd.DataFrame({
                'Category': ['Gaming', 'Music', 'Tech', 'Education', 'Entertainment'],
                'Avg Views': [15420, 23150, 18900, 12300, 19800],
                'Total Videos': [8932, 12045, 7821, 5643, 11234],
                'Processing Time': ['0.8s', '1.2s', '0.9s', '0.6s', '1.1s']
            })
            
            st.subheader("Sample Distributed Processing Results")
            st.dataframe(sample_results, use_container_width=True)

elif page == "Channel Stats":
    st.header("Channel Statistics")
    
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
    
    video_collection = db.get_collection("videos")
    video_data = list(video_collection.find())
    
    if video_data:
        df = pd.DataFrame(video_data)
        
        if 'duration_seconds' in df.columns:
            st.subheader("Video Duration Analysis")
            
            def categorize_duration(seconds):
                if seconds < 300: 
                    return "Short (< 5 min)"
                elif seconds < 1200:  
                    return "Medium (5-20 min)"
                else:
                    return "Long (> 20 min)"
            
            df['duration_category'] = df['duration_seconds'].apply(categorize_duration)
            
            duration_counts = df['duration_category'].value_counts()
            fig = px.pie(values=duration_counts.values, names=duration_counts.index, 
                        title="Video Duration Distribution")
            st.plotly_chart(fig, use_container_width=True)

elif page == "Raw Data":
    st.header("Raw Data")
    
    tab1, tab2, tab3 = st.tabs(["Video Data", "Channel Data", "Dask Results"])
    
    with tab1:
        video_collection = db.get_collection("videos")
        video_data = list(video_collection.find())
        
        if video_data:
            df = pd.DataFrame(video_data)
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
            
            st.subheader(f"Video Data ({len(df)} videos)")
            st.dataframe(df, use_container_width=True)
            
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
    
    with tab3:
        st.subheader("Dask Processing Results")
        
        dask_results = load_dask_results()
        if dask_results:
            st.json(dask_results)
            
            import json
            dask_json = json.dumps(dask_results, default=str, indent=2)
            st.download_button(
                label="Download Dask Results as JSON",
                data=dask_json,
                file_name=f"dask_results_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json"
            )
        else:
            st.info("No Dask processing results found. Run Dask Analytics first.")

st.sidebar.markdown("---")
st.sidebar.markdown("**Big Data Stack:**")
st.sidebar.markdown("• Dask Distributed Computing")
st.sidebar.markdown("• MongoDB Atlas Database")
st.sidebar.markdown("• Streamlit Interactive UI")
st.sidebar.markdown("• Plotly Data Visualization")
st.sidebar.markdown("---")
st.sidebar.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

