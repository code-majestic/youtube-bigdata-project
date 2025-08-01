from flask import Flask, render_template, jsonify, request
import dask.dataframe as dd
import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import numpy as np
from datetime import datetime
import json

load_dotenv()

app = Flask(__name__)

class YouTubeDaskAnalytics:
    def __init__(self):
        self.mongodb_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client['youtube_data']
        print("🚀 Dask Analytics Initialized for Web!")

    def load_and_process_data(self):  
        """Load data and convert to Dask DataFrame for big data processing"""  
        try:   
            videos_collection = self.db['videos']  
            videos_data = list(videos_collection.find())  
              
            if not videos_data:  
                return None  
                
            df = pd.DataFrame(videos_data)  
               
            if 'snippet' in df.columns:  
                snippet_df = pd.json_normalize(df['snippet'])  
                snippet_df.columns = ['snippet_' + col for col in snippet_df.columns]  
                df = pd.concat([df.drop('snippet', axis=1), snippet_df], axis=1)  
                
            dask_df = dd.from_pandas(df, npartitions=4)  
            return dask_df  
              
        except Exception as e:  
            print(f"Error loading data: {e}")  
            return None  
    
    def get_web_analytics(self, dask_df):
        """Get analytics data formatted for web display"""
        try: 
            dask_df['view_count'] = dd.to_numeric(dask_df['view_count'], errors='coerce')  
            dask_df['like_count'] = dd.to_numeric(dask_df['like_count'], errors='coerce')  
            dask_df['engagement_rate'] = dd.to_numeric(dask_df['engagement_rate'], errors='coerce')  
            dask_df['duration_seconds'] = dd.to_numeric(dask_df['duration_seconds'], errors='coerce')  
            
            total_videos = len(dask_df)
            avg_views = dask_df['view_count'].mean().compute()
            max_views = dask_df['view_count'].max().compute()
            min_views = dask_df['view_count'].min().compute()
            total_views = dask_df['view_count'].sum().compute()
            avg_engagement = dask_df['engagement_rate'].mean().compute()
            
            available_cols = ['view_count']  
            if 'snippet_title' in dask_df.columns:  
                available_cols.append('snippet_title')  
            if 'like_count' in dask_df.columns:  
                available_cols.append('like_count')  
            if 'engagement_rate' in dask_df.columns:  
                available_cols.append('engagement_rate')  
            
            top_videos_df = dask_df.nlargest(10, 'view_count')[available_cols].compute()
            
            top_videos = []
            for idx, row in top_videos_df.iterrows():
                if not pd.isna(row['view_count']):
                    video_data = {
                        'title': str(row.get('snippet_title', 'N/A'))[:50] + "..." if len(str(row.get('snippet_title', 'N/A'))) > 50 else str(row.get('snippet_title', 'N/A')),
                        'views': int(row['view_count']),
                        'likes': int(row.get('like_count', 0)),
                        'engagement': float(row.get('engagement_rate', 0))
                    }
                    top_videos.append(video_data)
            
            percentiles = dask_df['view_count'].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).compute()
            
            return {
                'success': True,
                'metrics': {
                    'total_videos': int(total_videos),
                    'total_views': int(total_views),
                    'avg_views': int(avg_views),
                    'max_views': int(max_views),
                    'min_views': int(min_views),
                    'avg_engagement': float(avg_engagement)
                },
                'top_videos': top_videos,
                'percentiles': {
                    '25th': int(percentiles[0.25]),
                    '50th': int(percentiles[0.5]),
                    '75th': int(percentiles[0.75]),
                    '90th': int(percentiles[0.9]),
                    '95th': int(percentiles[0.95]),
                    '99th': int(percentiles[0.99])
                }
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def simulate_big_data_demo(self):
        """Simulate big data processing for demo"""
        try:
            n_videos = 10000  
            np.random.seed(42)
            
            views = np.concatenate([
                np.random.exponential(1000, int(n_videos * 0.7)),
                np.random.exponential(10000, int(n_videos * 0.25)),
                np.random.exponential(100000, int(n_videos * 0.05))
            ])
            
            large_data = {
                'video_id': [f'demo_video_{i:06d}' for i in range(n_videos)],
                'views': views[:n_videos].astype(int),
                'likes': (views[:n_videos] * np.random.uniform(0.01, 0.05, n_videos)).astype(int),
                'category': np.random.choice([
                    'Gaming', 'Music', 'Entertainment', 'Education', 'Tech',
                    'Sports', 'News', 'Comedy', 'Lifestyle', 'Travel'
                ], n_videos),
                'upload_hour': np.random.randint(0, 24, n_videos)
            }
            
            large_df = pd.DataFrame(large_data)
            large_df['engagement_rate'] = (large_df['likes'] / large_df['views']) * 100
            
            dask_large_df = dd.from_pandas(large_df, npartitions=5)
            
            # Analytics
            total_views = dask_large_df['views'].sum().compute()
            avg_views = dask_large_df['views'].mean().compute()
            viral_videos = (dask_large_df['views'] > 100000).sum().compute()
            
            category_stats = dask_large_df.groupby('category')['views'].agg(['count', 'mean', 'sum']).compute()
            category_data = []
            for category, row in category_stats.iterrows():
                category_data.append({
                    'category': category,
                    'count': int(row['count']),
                    'avg_views': int(row['mean']),
                    'total_views': int(row['sum'])
                })
            
            # Best upload hours
            hour_performance = dask_large_df.groupby('upload_hour')['views'].mean().compute()
            hour_data = [{'hour': int(hour), 'avg_views': int(views)} for hour, views in hour_performance.items()]
            hour_data = sorted(hour_data, key=lambda x: x['avg_views'], reverse=True)
            
            return {
                'success': True,
                'demo_metrics': {
                    'total_videos': n_videos,
                    'total_views': int(total_views),
                    'avg_views': int(avg_views),
                    'viral_videos': int(viral_videos),
                    'viral_percentage': round(viral_videos/n_videos*100, 1)
                },
                'category_performance': category_data[:5],  # Top 5
                'best_hours': hour_data[:5]  # Top 5
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}

analytics = YouTubeDaskAnalytics()

@app.route('/')
def home():
    return render_template('dask_analytics.html')

@app.route('/dask-analytics')
def dask_analytics_page():
    return render_template('dask_analytics.html')

@app.route('/api/dask-analytics')
def api_dask_analytics():
    """API endpoint for real data analytics"""
    try:
        dask_df = analytics.load_and_process_data()
        if dask_df is not None:
            results = analytics.get_web_analytics(dask_df)
            return jsonify(results)
        else:
            return jsonify({'success': False, 'error': 'No data found in MongoDB'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/big-data-demo')
def api_big_data_demo():
    """API endpoint for big data simulation"""
    try:
        results = analytics.simulate_big_data_demo()
        return jsonify(results)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/dashboard-stats')
def api_dashboard_stats():
    """Combined dashboard statistics"""
    try:
        real_data = {'success': False}
        dask_df = analytics.load_and_process_data()
        if dask_df is not None:
            real_data = analytics.get_web_analytics(dask_df)
        
        demo_data = analytics.simulate_big_data_demo()
        
        return jsonify({
            'real_data': real_data,
            'demo_data': demo_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    print("Starting YouTube Big Data Analytics Web App")
    print("Open: http://localhost:5000")
    print("Dask Analytics: http://localhost:5000/dask-analytics")
    app.run(debug=True, host='0.0.0.0', port=5000)