import time
from pymongo import MongoClient
import pandas as pd
import datetime as dt
from tqdm import tqdm

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads
all_comments = client.resilient.all
newcomers = client.resilient.newcomers

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

start = time.time()

misses = 0
hits = 0

threads = get_popular_threads(
    filter={'newcomer_utc': {'$ne': None}},
    columns=['subreddit', 'newcomer_comments', 'newcomers', 'comments']
)

for thread in tqdm(threads.itertuples(), total=len(threads)):
    for newcomer, comment_id in zip(thread.newcomers, thread.newcomer_comments):
        while True:
            try:
                comments = pd.DataFrame(all_comments.find(
                    {'subreddit': thread.subreddit, 'author': newcomer},
                    {
                        'author': 1,
                        'id': 1,
                        'subreddit': 1,
                        'removed': 1,
                        'body': 1,
                        'created_utc': 1
                    }
                ))

                if len(comments) == 0:
                    misses += 1
                    newcomers.update_one({'author': newcomer, 'subreddit': thread.subreddit}, {'$set': {
                        'author': newcomer,
                        'subreddit': thread.subreddit,
                        'missed': True,
                        'comment_missed': comment_id
                    }})
                    break

                newcomers.update_one({'author': newcomer, 'subreddit': thread.subreddit}, {'$set': {
                    'author': newcomer,
                    'joined': thread.subreddit,
                    'joined_thread': thread.Index,
                    'joined_at': comments['created_utc'].min(),
                    'last_seen': comments['created_utc'].max(),
                    'tenure': comments['created_utc'].max() - comments['created_utc'].min(),
                    'comments': [{
                        'id': comment.id,
                        'created_utc': comment.created_utc,
                        'body': comment.body,
                        'removed': comment.removed,
                    } for comment in comments.itertuples()],
                    'num_comments': len(comments),
                    'first_removed': bool(comments[comments['created_utc'] == comments['created_utc'].min()] \
                        ['removed'].values[0]),
                    'num_removed': len(comments[comments['removed']]) 
                }}, upsert=True)

                hits += 1

                break
            except Exception as e:
                print(e)
                time.sleep(10)
                continue

print(f'#hits: {hits} | #misses: {misses} | %: {hits / (hits + misses)}')
print(f'Duration: {str(dt.timedelta(seconds=time.time() - start))}')

    