import dask.dataframe as dd
import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

load_dotenv()

class YouTubeDaskAnalytics:
    def __init__(self):
        self.mongodb_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client['youtube_data']
        print("Dask Analytics Initialized (No Java Required!)")
        print("Ready for Big Data Processing!")
    
    def save_results_to_mongodb(self, results):
        """Save Dask processing results to MongoDB"""
        try:
            print("\n Saving results to MongoDB...")
            
            dask_results_collection = self.db['dask_results']
            
            result_doc = {
                'timestamp': datetime.now(),
                'processing_method': 'Dask Distributed Computing',
                'dataset_size': results.get('total_videos', 0),
                'partitions': 10,
                'results': results,
                'status': 'completed'
            }
            
            dask_results_collection.delete_many({})
            dask_results_collection.insert_one(result_doc)
            
            print("Results saved to MongoDB successfully!")
            return True
            
        except Exception as e:
            print(f"Error saving results: {e}")
            return False
    
    def load_and_process_data(self):
        """Load data and convert to Dask DataFrame for big data processing"""
        try:
            print("\n Loading data from MongoDB...")
            
            videos_collection = self.db['videos']
            videos_data = list(videos_collection.find())
            
            if not videos_data:
                print("No video data found")
                return None
            
            # Create pandas DataFrame
            df = pd.DataFrame(videos_data)
            print(f"Loaded {len(df)} videos from MongoDB")
            
            if 'snippet' in df.columns:
                snippet_df = pd.json_normalize(df['snippet'])
                snippet_df.columns = ['snippet_' + col for col in snippet_df.columns]
                df = pd.concat([df.drop('snippet', axis=1), snippet_df], axis=1)
                print("Flattened snippet data")
            
            dask_df = dd.from_pandas(df, npartitions=4)
            
            print(f"Created Dask DataFrame with {len(df)} videos")
            print("Ready for distributed computing!")
            return dask_df
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    def big_data_analytics(self, dask_df):
        """Perform big data analytics using Dask"""
        try:
            print("\n" + "="*60)
            print("BIG DATA ANALYTICS WITH DASK")
            print("="*60)
    
            print("Processing numeric columns...")
            dask_df['view_count'] = dd.to_numeric(dask_df['view_count'], errors='coerce')
            dask_df['like_count'] = dd.to_numeric(dask_df['like_count'], errors='coerce')
            dask_df['engagement_rate'] = dd.to_numeric(dask_df['engagement_rate'], errors='coerce')
            dask_df['duration_seconds'] = dd.to_numeric(dask_df['duration_seconds'], errors='coerce')
            
            print("\n PERFORMANCE METRICS (Distributed Computing):")
            print("-" * 50)
            
            avg_views = dask_df['view_count'].mean().compute()
            max_views = dask_df['view_count'].max().compute()
            min_views = dask_df['view_count'].min().compute()
            total_views = dask_df['view_count'].sum().compute()
            total_videos = len(dask_df)
            avg_engagement = dask_df['engagement_rate'].mean().compute()
            
            print(f"Total Videos Processed: {total_videos:,}")
            print(f"Total Views: {total_views:,.0f}")
            print(f"Average Views per Video: {avg_views:,.0f}")
            print(f"Maximum Views: {max_views:,.0f}")
            print(f"Minimum Views: {min_views:,.0f}")
            print(f"Average Engagement Rate: {avg_engagement:.2f}%")
            
            print("\n TOP 5 PERFORMING VIDEOS:")
            print("-" * 40)
            
            available_cols = ['view_count']
            if 'snippet_title' in dask_df.columns:
                available_cols.append('snippet_title')
            if 'like_count' in dask_df.columns:
                available_cols.append('like_count')
            if 'engagement_rate' in dask_df.columns:
                available_cols.append('engagement_rate')
            
            top_videos = dask_df.nlargest(5, 'view_count')[available_cols].compute()
            
            for idx, row in top_videos.iterrows():
                title = row.get('snippet_title', 'N/A')[:50] + "..." if len(str(row.get('snippet_title', 'N/A'))) > 50 else row.get('snippet_title', 'N/A')
                views = row['view_count']
                likes = row.get('like_count', 0)
                engagement = row.get('engagement_rate', 0)
                
                print(f"{title}")
                print(f"Views: {views:,.0f} | Likes: {likes:,.0f} | Engagement: {engagement:.2f}%")
                print()
            
            return {
                'total_videos': total_videos,
                'total_views': total_views,
                'avg_views': avg_views,
                'max_views': max_views,
                'min_views': min_views,
                'avg_engagement': avg_engagement,
                'top_videos': top_videos.to_dict('records')
            }
            
        except Exception as e:
            print(f"Error in analytics: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def simulate_large_dataset_processing(self):
        """Simulate processing of large dataset - Big Data Demo"""
        print("\n" + "="*60)
        print("LARGE DATASET SIMULATION - BIG DATA DEMO")
        print("="*60)
        
        n_videos = 50000  
        
        print(f"Generating {n_videos:,} synthetic video records...")
        print("Simulating real-world YouTube dataset...")
        
        np.random.seed(42)  
        
        views = np.concatenate([
            np.random.exponential(1000, int(n_videos * 0.7)),      
            np.random.exponential(10000, int(n_videos * 0.25)),      
            np.random.exponential(100000, int(n_videos * 0.05))    
        ])
        
        large_data = {
            'video_id': [f'video_{i:06d}' for i in range(n_videos)],
            'views': views[:n_videos].astype(int),
            'likes': (views[:n_videos] * np.random.uniform(0.01, 0.05, n_videos)).astype(int),
            'duration': np.random.normal(300, 120, n_videos).clip(30, 3600).astype(int),
            'category': np.random.choice([
                'Gaming', 'Music', 'Entertainment', 'Education', 'Tech', 
                'Sports', 'News', 'Comedy', 'Lifestyle', 'Travel'
            ], n_videos),
            'upload_hour': np.random.randint(0, 24, n_videos),
            'upload_day': np.random.choice(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], n_videos)
        }
        
        large_df = pd.DataFrame(large_data)
        large_df['engagement_rate'] = (large_df['likes'] / large_df['views']) * 100
        
        print(f"Created dataset with {len(large_df):,} records")
        print(f"Dataset size: ~{len(large_df) * 10 / 1024 / 1024:.1f} MB (simulated)")
        
        print("Converting to Dask DataFrame for distributed processing...")
        dask_large_df = dd.from_pandas(large_df, npartitions=10)
        
        print("Dask DataFrame created with 10 partitions")
        print("Starting Big Data Analytics...")
        
        print("\n BIG DATA ANALYTICS RESULTS:")
        print("-" * 40)
    
        total_views = dask_large_df['views'].sum().compute()
        avg_views = dask_large_df['views'].mean().compute()
        viral_threshold = 100000
        viral_videos = (dask_large_df['views'] > viral_threshold).sum().compute()
        
        print(f"Total Videos: {n_videos:,}")
        print(f"Total Views: {total_views:,}")
        print(f"Average Views: {avg_views:,.0f}")
        print(f"Viral Videos (>{viral_threshold:,} views): {viral_videos:,} ({viral_videos/n_videos*100:.1f}%)")
        
        print("\n CATEGORY PERFORMANCE (Distributed Computing):")
        print("-" * 50)
        
        category_stats = dask_large_df.groupby('category').agg({
            'views': ['count', 'mean', 'sum', 'max'],
            'engagement_rate': 'mean',
            'duration': 'mean'
        }).compute()
        
        category_stats = category_stats.sort_values(('views', 'mean'), ascending=False)
        print(category_stats.head())
        
        print("\n Large dataset processing completed!")
        print("Dask processed 50K records using distributed computing!")
        
        return {
            'simulation_total_videos': n_videos,
            'simulation_total_views': total_views,
            'simulation_avg_views': avg_views,
            'simulation_viral_videos': viral_videos,
            'category_performance': category_stats.head().to_dict()
        }

def main():
    print("Starting YouTube Big Data Analytics with Dask")
    print("="*60)
    
    analytics = YouTubeDaskAnalytics()
    
    try:
        all_results = {
            'processing_timestamp': datetime.now().isoformat(),
            'status': 'running'
        }
        
        print("\n PHASE 1: Analyzing Real YouTube Data")
        dask_df = analytics.load_and_process_data()
        
        if dask_df is not None:
            real_results = analytics.big_data_analytics(dask_df)
            if real_results:
                all_results.update(real_results)
        else:
            print("No real data found, proceeding to simulation...")
        
        print("\n PHASE 2: Big Data Simulation")
        simulation_results = analytics.simulate_large_dataset_processing()
        
        if simulation_results:
            all_results.update(simulation_results)
        
        all_results['status'] = 'completed'
        
        print("\n🔹 PHASE 3: Saving Results")
        save_success = analytics.save_results_to_mongodb(all_results)
        
        if save_success:
            print("\n" + "="*60)
            print("BIG DATA ANALYTICS COMPLETED SUCCESSFULLY!")
            print("Dask processing demonstrated")
            print("Distributed computing showcased") 
            print("Large-scale data analysis performed")
            print("Results saved to MongoDB")
            print("="*60)
        else:
            print("Analytics completed but results saving failed")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()