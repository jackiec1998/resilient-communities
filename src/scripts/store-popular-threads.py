from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import time

client = MongoClient('localhost', 27017)
popular_snapshots = client.resilient.popular
popular_threads = client.resilient.popular_threads

previous_count = popular_threads.estimated_document_count()

pipeline = [
    {'$sort': {'retrieved_utc': 1}},
    {'$group': {
        '_id': '$id',
        'subreddit': {'$first': '$subreddit'},
        'created_utc': {'$first': '$created_utc'},
        'title': {'$first': '$title'},
        'domain': {'$first': '$domain'},
        'permalink': {'$first': '$permalink'},
        'is_self': {'$first': '$is_self'},
        'author': {'$first': '$author'},
        'selftext': {'$first': '$selftext'},
        'is_video': {'$first': '$is_video'},
        'min_utc': {'$min': '$retrieved_utc'},
        'max_utc': {'$max': '$retrieved_utc'},
        'snapshots': {'$push': {
            'retrieved_utc': '$retrieved_utc',
            'rank': '$rank',
            'score': '$score',
            'upvote_ratio': '$upvote_ratio',
            'num_comments': '$num_comments'
        }}
    }}
]

threads = pd.DataFrame(
    popular_snapshots.aggregate(pipeline, allowDiskUse=True)
).rename(columns={'_id': 'id'}).set_index('id')

for id, thread in tqdm(threads.iterrows(), total=len(threads)):
    
    features = thread.to_dict()
    
    features['created_to_popular'] = features['min_utc'] - features['created_utc']
    features['popular_window'] = features['max_utc'] - features['min_utc']
    
    popular_threads.update_one({'id': id}, {'$set': features}, upsert=True)

current_count = popular_threads.estimated_document_count()

print(f'{current_count - previous_count:,} new r/popular threads.')