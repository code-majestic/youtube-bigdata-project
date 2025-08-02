import dask.dataframe as dd
import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime
import json
import time

# Load environment variables
load_dotenv()

class YouTubeDaskAnalytics:
    def __init__(self):
        """Initialize Dask Analytics for YouTube Big Data"""
        self.mongodb_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client['youtube_data']
        print("YouTube Dask Analytics Initialized!")
        
    def load_and_process_data(self):
        """Load MongoDB data and convert to Dask DataFrame"""
        try:
            print("Loading data from MongoDB...")
            
            videos_collection = self.db['videos']
            videos_data = list(videos_collection.find())
            
            if not videos_data:
                print("No video data found in MongoDB")
                return None
                
            print(f"Loaded {len(videos_data)} videos from MongoDB")
            
            df = pd.DataFrame(videos_data)
            
            if 'snippet' in df.columns:
                snippet_df = pd.json_normalize(df['snippet'])
                snippet_df.columns = ['snippet_' + col for col in snippet_df.columns]
                df = pd.concat([df.drop('snippet', axis=1), snippet_df], axis=1)
            
            if 'statistics' in df.columns:
                stats_df = pd.json_normalize(df['statistics'])
                stats_df.columns = ['stats_' + col for col in stats_df.columns]
                df = pd.concat([df.drop('statistics', axis=1), stats_df], axis=1)
            
            print("Converting to Dask DataFrame for distributed processing...")
            dask_df = dd.from_pandas(df, npartitions=4)
            
            print("Dask DataFrame created successfully!")
            return dask_df
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    def perform_dask_analytics(self, dask_df):
        """Perform distributed analytics using Dask"""
        try:
            print("Starting Dask Distributed Analytics...")
            
            numeric_columns = ['view_count', 'like_count', 'comment_count', 'engagement_rate', 'duration_seconds']
            for col in numeric_columns:
                if col in dask_df.columns:
                    dask_df[col] = dd.to_numeric(dask_df[col], errors='coerce')
            
            print("Computing basic statistics with Dask...")
            
            metrics = {}
            metrics['total_videos'] = len(dask_df)
            
            if 'view_count' in dask_df.columns:
                metrics['total_views'] = int(dask_df['view_count'].sum().compute())
                metrics['avg_views'] = int(dask_df['view_count'].mean().compute())
                metrics['max_views'] = int(dask_df['view_count'].max().compute())
                metrics['min_views'] = int(dask_df['view_count'].min().compute())
                metrics['median_views'] = int(dask_df['view_count'].quantile(0.5).compute())
            
            if 'engagement_rate' in dask_df.columns:
                metrics['avg_engagement'] = float(dask_df['engagement_rate'].mean().compute())
                metrics['max_engagement'] = float(dask_df['engagement_rate'].max().compute())
            
            if 'duration_seconds' in dask_df.columns:
                metrics['avg_duration'] = int(dask_df['duration_seconds'].mean().compute())
            
            print("Computing advanced analytics...")
            
            if 'view_count' in dask_df.columns:
                percentiles = dask_df['view_count'].quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).compute()
                metrics['percentiles'] = {
                    '10th': int(percentiles[0.1]),
                    '25th': int(percentiles[0.25]),
                    '50th': int(percentiles[0.5]),
                    '75th': int(percentiles[0.75]),
                    '90th': int(percentiles[0.9]),
                    '95th': int(percentiles[0.95]),
                    '99th': int(percentiles[0.99])
                }
            
            print("🏆 Finding top performing videos...")
            top_videos = []
            
            if 'view_count' in dask_df.columns:
                top_cols = ['view_count']
                if 'snippet_title' in dask_df.columns:
                    top_cols.append('snippet_title')
                if 'like_count' in dask_df.columns:
                    top_cols.append('like_count')
                if 'engagement_rate' in dask_df.columns:
                    top_cols.append('engagement_rate')
                
                top_videos_df = dask_df.nlargest(10, 'view_count')[top_cols].compute()
                
                for idx, row in top_videos_df.iterrows():
                    if not pd.isna(row['view_count']):
                        video_data = {
                            'title': str(row.get('snippet_title', 'Unknown Title'))[:50] + ("..." if len(str(row.get('snippet_title', ''))) > 50 else ""),
                            'views': int(row['view_count']),
                            'likes': int(row.get('like_count', 0)),
                            'engagement': round(float(row.get('engagement_rate', 0)), 2)
                        }
                        top_videos.append(video_data)
            
            category_analysis = []
            if 'snippet_categoryId' in dask_df.columns and 'view_count' in dask_df.columns:
                print("Analyzing categories...")
                category_stats = dask_df.groupby('snippet_categoryId')['view_count'].agg(['count', 'mean', 'sum']).compute()
                
                for category_id, row in category_stats.head(5).iterrows():
                    category_analysis.append({
                        'category_id': str(category_id),
                        'video_count': int(row['count']),
                        'avg_views': int(row['mean']),
                        'total_views': int(row['sum'])
                    })
            
            performance_tiers = {}
            if 'view_count' in dask_df.columns:
                print("🎖️ Classifying performance tiers...")
                
                viral_threshold = metrics.get('percentiles', {}).get('95th', 100000)
                popular_threshold = metrics.get('percentiles', {}).get('75th', 10000)
                
                viral_videos = (dask_df['view_count'] >= viral_threshold).sum().compute()
                popular_videos = ((dask_df['view_count'] >= popular_threshold) & 
                                (dask_df['view_count'] < viral_threshold)).sum().compute()
                regular_videos = (dask_df['view_count'] < popular_threshold).sum().compute()
                
                performance_tiers = {
                    'viral': {'count': int(viral_videos), 'threshold': viral_threshold},
                    'popular': {'count': int(popular_videos), 'threshold': popular_threshold},
                    'regular': {'count': int(regular_videos), 'threshold': 0}
                }
            
            results = {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'processing_method': 'Dask Distributed Computing',
                'dataset_info': {
                    'total_records': metrics['total_videos'],
                    'partitions': dask_df.npartitions,
                    'columns': list(dask_df.columns)
                },
                'metrics': metrics,
                'top_videos': top_videos,
                'category_analysis': category_analysis,
                'performance_tiers': performance_tiers
            }
            
            print("Dask Analytics completed successfully!")
            return results
            
        except Exception as e:
            print(f"❌ Error in Dask analytics: {e}")
            return {'success': False, 'error': str(e)}
    
    def simulate_big_data_processing(self):
        """Simulate big data processing with large synthetic dataset"""
        try:
            print("Starting Big Data Simulation (50K+ records)...")
            
            n_videos = 50000
            np.random.seed(42)
            
            print(f"📊 Generating {n_videos:,} synthetic video records...")
            
            views = np.concatenate([
                np.random.exponential(1000, int(n_videos * 0.6)),    
                np.random.exponential(10000, int(n_videos * 0.3)),    
                np.random.exponential(100000, int(n_videos * 0.08)), 
                np.random.exponential(1000000, int(n_videos * 0.02))  
            ])
            
            categories = ['Gaming', 'Music', 'Entertainment', 'Education', 'Technology', 
                         'Sports', 'News', 'Comedy', 'Lifestyle', 'Travel', 'Food', 'Science']
            
            synthetic_data = {
                'video_id': [f'synthetic_video_{i:06d}' for i in range(n_videos)],
                'views': views[:n_videos].astype(int),
                'category': np.random.choice(categories, n_videos),
                'upload_hour': np.random.randint(0, 24, n_videos),
                'upload_day': np.random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                                              'Friday', 'Saturday', 'Sunday'], n_videos),
                'duration_minutes': np.random.exponential(8, n_videos),  # Average 8 minutes
                'upload_year': np.random.choice([2020, 2021, 2022, 2023, 2024], n_videos)
            }
            
            synthetic_data['likes'] = (synthetic_data['views'] * np.random.uniform(0.01, 0.06, n_videos)).astype(int)
            synthetic_data['comments'] = (synthetic_data['views'] * np.random.uniform(0.001, 0.01, n_videos)).astype(int)
            synthetic_data['engagement_rate'] = ((synthetic_data['likes'] + synthetic_data['comments']) / 
                                               synthetic_data['views']) * 100
            
            print("Converting to Dask DataFrame...")
            large_df = pd.DataFrame(synthetic_data)
            dask_large_df = dd.from_pandas(large_df, npartitions=10)
            
            print("Performing distributed analytics on big data...")
            
            start_time = time.time()
            
            total_views = dask_large_df['views'].sum().compute()
            avg_views = dask_large_df['views'].mean().compute()
            median_views = dask_large_df['views'].quantile(0.5).compute()
            
            viral_videos = (dask_large_df['views'] > 100000).sum().compute()
            super_viral = (dask_large_df['views'] > 1000000).sum().compute()
            
            category_performance = dask_large_df.groupby('category').agg({
                'views': ['count', 'mean', 'sum', 'max'],
                'engagement_rate': 'mean'
            }).compute()
            
            category_results = []
            for category in category_performance.index:
                category_results.append({
                    'category': category,
                    'video_count': int(category_performance.loc[category, ('views', 'count')]),
                    'avg_views': int(category_performance.loc[category, ('views', 'mean')]),
                    'total_views': int(category_performance.loc[category, ('views', 'sum')]),
                    'max_views': int(category_performance.loc[category, ('views', 'max')]),
                    'avg_engagement': round(category_performance.loc[category, ('engagement_rate', 'mean')], 2)
                })
            
            hour_performance = dask_large_df.groupby('upload_hour')['views'].mean().compute()
            best_hours = [{'hour': int(hour), 'avg_views': int(views)} 
                         for hour, views in hour_performance.items()]
            best_hours = sorted(best_hours, key=lambda x: x['avg_views'], reverse=True)
            
            day_performance = dask_large_df.groupby('upload_day')['views'].mean().compute()
            best_days = [{'day': day, 'avg_views': int(views)} 
                        for day, views in day_performance.items()]
            best_days = sorted(best_days, key=lambda x: x['avg_views'], reverse=True)
            
            processing_time = time.time() - start_time
            
            results = {
                'success': True,
                'processing_info': {
                    'dataset_size': n_videos,
                    'partitions': 10,
                    'processing_time_seconds': round(processing_time, 2),
                    'records_per_second': int(n_videos / processing_time)
                },
                'big_data_metrics': {
                    'total_videos': n_videos,
                    'total_views': int(total_views),
                    'avg_views': int(avg_views),
                    'median_views': int(median_views),
                    'viral_videos': int(viral_videos),
                    'super_viral_videos': int(super_viral),
                    'viral_percentage': round((viral_videos / n_videos) * 100, 2)
                },
                'category_performance': sorted(category_results, key=lambda x: x['avg_views'], reverse=True),
                'best_upload_hours': best_hours[:5],
                'best_upload_days': best_days,
                'performance_distribution': {
                    'low_performance': int((dask_large_df['views'] < 1000).sum().compute()),
                    'medium_performance': int(((dask_large_df['views'] >= 1000) & 
                                             (dask_large_df['views'] < 10000)).sum().compute()),
                    'high_performance': int(((dask_large_df['views'] >= 10000) & 
                                           (dask_large_df['views'] < 100000)).sum().compute()),
                    'viral_performance': int((dask_large_df['views'] >= 100000).sum().compute())
                }
            }
            
            print(f"Big Data Analytics completed in {processing_time:.2f} seconds!")
            print(f"Processed {n_videos:,} records at {int(n_videos/processing_time):,} records/second")
            
            return results
            
        except Exception as e:
            print(f"Error in big data simulation: {e}")
            return {'success': False, 'error': str(e)}
    
    def save_results_to_mongodb(self, results, collection_name='dask_results'):
        """Save Dask analytics results to MongoDB"""
        try:
            collection = self.db[collection_name]
            
            # Add metadata
            results['saved_at'] = datetime.now()
            results['collection'] = collection_name
            
            # Insert results
            collection.insert_one(results)
            print(f"Results saved to MongoDB collection: {collection_name}")
            
            return True
            
        except Exception as e:
            print(f"Error saving results: {e}")
            return False
    
    def run_complete_analytics(self):
        """Run complete Dask analytics pipeline"""
        try:
            print("Starting Complete YouTube Dask Analytics Pipeline...")
            print("=" * 60)
            
            print("PHASE 1: Real Data Analytics")
            print("-" * 30)
            
            dask_df = self.load_and_process_data()
            real_results = None
            
            if dask_df is not None:
                real_results = self.perform_dask_analytics(dask_df)
                if real_results['success']:
                    self.save_results_to_mongodb(real_results, 'real_data_analytics')
            else:
                print("No real data found, skipping real data analytics")
            
            print("\n" + "=" * 60)
            
            print("PHASE 2: Big Data Simulation")
            print("-" * 30)
            
            simulation_results = self.simulate_big_data_processing()
            if simulation_results['success']:
                self.save_results_to_mongodb(simulation_results, 'big_data_simulation')
            
            print("\n" + "=" * 60)
            print("🎉 Complete Analytics Pipeline Finished!")
            
            # Summary
            print("\n SUMMARY:")
            if real_results and real_results['success']:
                print(f"Real Data: {real_results['dataset_info']['total_records']} videos analyzed")
            if simulation_results['success']:
                print(f"Big Data: {simulation_results['processing_info']['dataset_size']:,} synthetic records processed")
                print(f"Processing Speed: {simulation_results['processing_info']['records_per_second']:,} records/second")
            
            return {
                'real_data': real_results,
                'big_data_simulation': simulation_results,
                'pipeline_success': True
            }
            
        except Exception as e:
            print(f"Pipeline error: {e}")
            return {'pipeline_success': False, 'error': str(e)}

def main():
    """Main execution function"""
    print("YouTube Big Data Analytics with Dask")
    print("=" * 50)
    
    analytics = YouTubeDaskAnalytics()
    
    results = analytics.run_complete_analytics()
    
    if results['pipeline_success']:
        print("\n All analytics completed successfully!")
        
        if results.get('big_data_simulation', {}).get('success'):
            sim_data = results['big_data_simulation']
            print(f"\n KEY INSIGHTS:")
            print(f"• Processed {sim_data['processing_info']['dataset_size']:,} video records")
            print(f"• Found {sim_data['big_data_metrics']['viral_videos']:,} viral videos ({sim_data['big_data_metrics']['viral_percentage']}%)")
            print(f"• Average views per video: {sim_data['big_data_metrics']['avg_views']:,}")
            print(f"• Processing speed: {sim_data['processing_info']['records_per_second']:,} records/second")
            
            if sim_data.get('category_performance'):
                top_category = sim_data['category_performance'][0]
                print(f"• Top performing category: {top_category['category']} ({top_category['avg_views']:,} avg views)")
            
            if sim_data.get('best_upload_hours'):
                best_hour = sim_data['best_upload_hours'][0]
                print(f"• Best upload time: {best_hour['hour']}:00 ({best_hour['avg_views']:,} avg views)")
    else:
        print("Analytics pipeline failed!")
        if _name_ == "_main_":
           main()       