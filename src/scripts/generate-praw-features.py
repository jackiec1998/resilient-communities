from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import datetime as dt
import time
import argparse

def get_popular_threads(filter={}, columns=[], n=None):
    
    # Adding index to the query projection.
    fields = {field: 1 for field in ['id'] + columns}

    cursor = popular_threads.find(filter, fields)

    if n is not None:
        cursor = cursor.limit(n)

    return pd.DataFrame(cursor).set_index('id') \
        .drop(columns=['_id'])[columns]

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads
all_comments = client.resilient.all

parser = argparse.ArgumentParser(
    description='Generates features from PRAW comment stream.'
)

parser.add_argument(
    '--refresh',
    default=False,
    action='store_true'
)

# Take a flag to refresh only the ones where the created_utc
# and praw_utc is sufficiently small, e.g., three days.

args = parser.parse_args()

while True:
    if args.refresh:
        ids = get_popular_threads().index.to_list()
    else:
        ids = pd.DataFrame(popular_threads.aggregate([
            {'$project': {'id': 1, 'created_utc': 1, 'praw_utc': 1,
                'update_distance': {'$subtract': ['$praw_utc', '$created_utc']}}},
            {'$match': {'$or': [
                {'$and': [
                    {'update_distance': {'$lte': dt.timedelta(days=14).total_seconds()}},
                    {'created_utc': {'$lte': int((dt.datetime.now() - dt.timedelta(days=7)).timestamp())}}
                ]},
                {'praw_utc': None}
            ]}}
        ], allowDiskUse=True))['id'].to_list()

    incompleted_ids = []

    for id in tqdm(ids, total=len(ids)):
        
        fullname = 't3_' + id
        
        pipeline = [
            {'$project': {'id': 1,
                            'link_id': 1,
                            'created_utc': 1,
                            'author': 1}},
            {'$match': {'link_id': {'$eq': fullname}}},
            {'$group': {'_id': {'author': '$author', 'link_id': '$link_id'},
                        'first_utc': {'$min': '$created_utc'},
                        'last_utc': {'$max': '$created_utc'},
                        'num_comments': {'$sum': 1},
                        'comments': {'$push': {
                            'id': '$id',
                            'author': '$author',
                            'created_utc': '$created_utc'}}}},
            {'$group': {'_id': '$_id.link_id',
                        'num_comments': {'$sum': '$num_comments'},
                        'num_authors': {'$sum': 1},
                        'first_utc': {'$min': '$first_utc'},
                        'last_utc': {'$max': '$last_utc'},
                        'comments': {'$push': '$comments'}}}
        ]
        
        try:
            features = pd.DataFrame(
                all_comments.aggregate(pipeline, allowDiskUse=True)
            ).set_index('_id').iloc[0].to_dict()
        except Exception:
            incompleted_ids.append(id)
            continue

        # Flatten comments because they're grouped by authors.
        features['comments'] = [comment for lst in features['comments'] for comment in lst]
        
        features['num_comments'] = int(features['num_comments']) # numpy.int64 -> int
        features['num_authors'] = int(features['num_authors']) # numpy.int64 -> int
        features['first_utc'] = int(features['first_utc']) # float -> int
        features['last_utc'] = int(features['last_utc']) # float -> int
        
        features['active_window'] = features['last_utc'] - features['first_utc']
        features['praw_utc'] = int(time.time())

        while True:
            try:
                popular_threads.update_one({'id': id}, 
                    {'$set': features, '$unset': {'comment_ids': '', 'authors': ''}})
                break
            except Exception:
                continue

    print(f'{len(incompleted_ids):,} threads incompleted.')