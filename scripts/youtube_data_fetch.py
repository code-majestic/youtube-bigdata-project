import os
from dotenv import load_dotenv
from pymongo import MongoClient
from googleapiclient.discovery import build
from datetime import datetime
import isodate

load_dotenv()

def process_video_data(video):
    """Process and enhance video data"""
    enhanced_video = video.copy()
    
    # Add processed timestamp
    enhanced_video['processed_at'] = datetime.now()
    
    # Process duration from ISO 8601 format to seconds
    if 'contentDetails' in video and 'duration' in video['contentDetails']:
        duration_iso = video['contentDetails']['duration']
        try:
            duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())
            enhanced_video['duration_seconds'] = duration_seconds
            enhanced_video['duration_formatted'] = format_duration(duration_seconds)
        except:
            enhanced_video['duration_seconds'] = 0
            enhanced_video['duration_formatted'] = "Unknown"
    
    # Process published date
    if 'snippet' in video and 'publishedAt' in video['snippet']:
        published_str = video['snippet']['publishedAt']
        try:
            published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            enhanced_video['published_date'] = published_date
            enhanced_video['published_year'] = published_date.year
            enhanced_video['published_month'] = published_date.month
            enhanced_video['published_day_of_week'] = published_date.strftime('%A')
        except:
            pass
    
    # Process statistics with safe conversion
    if 'statistics' in video:
        stats = video['statistics']
        enhanced_video['view_count'] = safe_int_convert(stats.get('viewCount', 0))
        enhanced_video['like_count'] = safe_int_convert(stats.get('likeCount', 0))
        enhanced_video['comment_count'] = safe_int_convert(stats.get('commentCount', 0))
        
        # Calculate engagement rate
        views = enhanced_video['view_count']
        likes = enhanced_video['like_count']
        comments = enhanced_video['comment_count']
        
        if views > 0:
            enhanced_video['engagement_rate'] = ((likes + comments) / views) * 100
        else:
            enhanced_video['engagement_rate'] = 0
    
    # Process tags
    if 'snippet' in video and 'tags' in video['snippet']:
        enhanced_video['tag_count'] = len(video['snippet']['tags'])
    else:
        enhanced_video['tag_count'] = 0
    
    # Add title length
    if 'snippet' in video and 'title' in video['snippet']:
        enhanced_video['title_length'] = len(video['snippet']['title'])
    
    # Add description length
    if 'snippet' in video and 'description' in video['snippet']:
        enhanced_video['description_length'] = len(video['snippet']['description'])
    else:
        enhanced_video['description_length'] = 0
    
    return enhanced_video

def safe_int_convert(value):
    """Safely convert string to int"""
    try:
        return int(value) if value else 0
    except:
        return 0

def format_duration(seconds):
    """Format duration in HH:MM:SS format"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

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

    try:
        channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'
        
        # Enhanced Channel Data fetch
        channel_response = youtube.channels().list(
            part='snippet,statistics,contentDetails,brandingSettings',
            id=channel_id
        ).execute()

        if channel_response['items']:
            channel_data = channel_response['items'][0]
            
            # Add processed timestamp
            channel_data['processed_at'] = datetime.now()
            
            # Clear existing channel data and insert new
            channel_collection.delete_many({'id': channel_id})
            channel_collection.insert_one(channel_data)
            print(f"Inserted enhanced channel: {channel_data['snippet']['title']}")

        # Enhanced Video Data fetch
        # First get video IDs
        search_response = youtube.search().list(
            part='id',
            channelId=channel_id,
            maxResults=50,  # Increased from 5 to 50
            type='video'
        ).execute()

        video_ids = []
        for item in search_response['items']:
            if item['id']['kind'] == 'youtube#video':
                video_ids.append(item['id']['videoId'])

        # Now get detailed video information
        if video_ids:
            # Process videos in batches of 50 (API limit)
            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i:i+50]
                
                videos_response = youtube.videos().list(
                    part='snippet,statistics,contentDetails,status',
                    id=','.join(batch_ids)
                ).execute()

                for video in videos_response['items']:
                    # Enhance video data
                    enhanced_video = process_video_data(video)
                    
                    # Clear existing video and insert enhanced version
                    videos_collection.delete_many({'id': video['id']})
                    videos_collection.insert_one(enhanced_video)
                    print(f"Inserted enhanced video: {video['snippet']['title']}")

        print(f"Successfully processed {len(video_ids)} videos")

    except Exception as e:
        print(f"Error fetching/storing data: {e}")

if __name__ == "__main__":
    fetch_and_store_data()