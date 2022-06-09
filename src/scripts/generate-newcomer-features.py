from pymongo import MongoClient
import time
import pandas as pd
import datetime as dt
from tqdm import tqdm
import os

os.chdir('/shared/jackie/resilient-communities')

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads
pushshift_comments = client.pushshift_comments

def get_popular_threads(filter={}, columns=[], n=None):
    
    # Adding index to the query projection.
    fields = {field: 1 for field in ['id'] + columns}
    
    cursor = popular_threads.find(filter, fields)
    
    if n is not None:
        cursor = cursor.limit(n)
    
    df = pd.DataFrame(cursor)

    if len(df) == 0:
        return df

    return df.set_index('id') \
        .drop(columns=['_id'])[columns]

# Find the subreddits we're targeting.
popular_subreddits = set(pushshift_comments.list_collection_names()) \
    .intersection(set(open('sample.txt', 'r').read().split()))

available_subreddits = []

print('Finding up-to-date subreddits.')

# Find subreddits we're up-to-date with.
for subreddit in tqdm(popular_subreddits):
    latest_comment = pd.DataFrame(pushshift_comments[subreddit].aggregate([
        {'$project': {'created_utc': 1}},
        {'$group': {'_id': None, 'last_utc': {'$max': '$created_utc'}}}
    ], allowDiskUse=True))['last_utc'].values[0]

    up_to_date = time.time() - latest_comment < dt.timedelta(days=1).total_seconds()

    if up_to_date:
        available_subreddits.append(subreddit)

print(f'Finding newcomers for {len(available_subreddits):,} up-to-date subreddits.')

previous_count = len(get_popular_threads(
    filter={'newcomer_utc': {'$ne': None}}))

for subreddit in tqdm(available_subreddits):

    while True:
        try:
            first_comments = pd.DataFrame(pushshift_comments[subreddit].aggregate([
                {'$group': {'_id': '$author',
                            'first_comment': {'$min': {
                                'created_utc': '$created_utc',
                                'author': '$author',
                                'id': '$id',
                                'link_id': '$link_id'}}}},
                {'$replaceRoot': {'newRoot': '$first_comment'}}
            ], allowDiskUse=True)).set_index('id')
        
            break
        except Exception as e:
            print(e)
            time.sleep(5)
            continue

    first_comments['link_id'] = first_comments['link_id'].str[3:]

    subreddit_popular_threads = get_popular_threads(filter={'subreddit': subreddit}) \
        .index.to_list()

    # Update the r/popular threads by adding the newcomers comment IDs.
    for thread in subreddit_popular_threads:

        newcomer_comments = first_comments[first_comments['link_id'] == thread]
        newcomers = newcomer_comments['author'].to_list()
        num_newcomers = len(newcomer_comments)

        features = {
            'newcomer_comments': newcomer_comments.index.to_list(),
            'newcomers': newcomers,
            'num_newcomers': num_newcomers,
            'newcomer_utc': int(time.time())
        }

        while True:
            try:
                popular_threads.update_one({'id': thread}, {'$set': features})
                break
            except Exception as e:
                print(e)
                time.sleep(5)
                continue

current_count = len(get_popular_threads(
    filter={'newcomer_utc': {'$ne': None}}))

print(f'{current_count - previous_count:,} new threads with newcomer features.')