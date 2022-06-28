from pymongo import MongoClient
import time
import pandas as pd
import sys
from psaw import PushshiftAPI as psaw
import datetime as dt
from tqdm import tqdm
import warnings
import traceback

warnings.filterwarnings('ignore')

psaw = psaw()

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads
newcomer_comments = client.resilient.newcomer_comments
newcomer_users = client.resilient.newcomer_users

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

def store_newcomers(thread):

    thread_newcomers = thread.newcomers.copy()

    while len(thread_newcomers) != 0:

        batch = thread_newcomers[:100]
        thread_newcomers = thread_newcomers[100:]

        comments = pd.DataFrame(psaw.search_comments(
            author = batch,
            subreddit = thread.subreddit,
            filter = ['author', 'link_id', 'created_utc', 'body', 'id'],
            sort = 'asc'
        )).drop(columns=['d_', 'created'])

        comments['link_id'] = comments['link_id'].str[3:]

        # Add the comments to the respective collection.
        for comment in comments.to_dict('records'):
            newcomer_comments.update_one({'id': comment['id']}, {'$set':
                comment
            }, upsert=True)

        # Add the newcomer attributes to the respective collection.
        for author in comments['author'].unique():

            author_comments = comments[comments['author'] == author]

            newcomer_users.update_one({'author': author, 'subreddit': thread.subreddit}, {'$set': {
                'author': author,
                'subreddit': thread.subreddit,
                'link_id': thread.Index,
                'thread_created_utc': int(thread.created_utc),
                'first_comment_utc': int(author_comments['created_utc'].min()),
                'last_comment_utc': int(author_comments['created_utc'].max()),
                'tenure': int(author_comments['created_utc'].max() - \
                    author_comments['created_utc'].min()),
                'num_comments': len(author_comments)
            }}, upsert=True)

        popular_threads.update_one({'id': thread.Index}, {'$set': {
            'retrieved_newcomer_history_utc': int(time.time())
        }})

        return

if __name__ == '__main__':

    start = int(time.time())

    # Get threads with newcomers.
    threads = get_popular_threads(
        filter = {'flagged_newcomers_utc': {'$ne': None}},
        columns = ['subreddit', 'newcomers', 'created_utc']
    )

    # Check within newcomer user histories and remove
    # the ones you already collected.

    for thread in tqdm(threads.itertuples(), total=len(threads)):

        attempts = 0

        while True:
            try:
                store_newcomers(thread)
                break

            except KeyboardInterrupt:
                print('Detected keyboard interrupt.')
                sys.exit()

            except Exception as e:
                attempts += 1

                if attempts >= 10:
                    print(e)
                    traceback.print_exc()
                    break

    print(f'Script took: {dt.timedelta(seconds=int(time.time()) - start)}')
