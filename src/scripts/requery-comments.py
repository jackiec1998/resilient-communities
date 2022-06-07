from pymongo import MongoClient
import praw
import os
import argparse
import time
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(
    description='Requeries the comment IDs found in the r/popular thread collection.'
)

parser.add_argument(
    '--refresh',
    default=False,
    action='store_true'
)

REFRESH = False

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads

reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT_ID_2'),
    client_secret=os.environ.get('REDDIT_CLIENT_SECRET_2'),
    user_agent=os.environ.get('REDDIT_USER_AGENT_2'),
    username=os.environ.get('REDDIT_USERNAME_2'),
    password=os.environ.get('REDDIT_PASSWORD_2')
)

def get_popular_threads(filter={}, columns=['id'], n=None):
    columns = {field: 1 for field in columns}

    cursor = popular_threads.find(filter, columns)

    if n is not None:
        cursor = cursor.limit(n)

    return pd.DataFrame(cursor).set_index('id') \
        .drop(columns=['_id'])

if REFRESH:
    ids = get_popular_threads(filter={'comments': {'$ne': None}}) \
        .index.to_list()
else:
    ids = get_popular_threads(filter={'comments' : {'$ne': None}, 
        'requery_utc': None}).index.to_list()

for id in tqdm(ids):

    while True:
        try:
            comments = pd.DataFrame(
                popular_threads.find_one({'id': id}, {'comments': 1})
            )['comments'].to_list()

            removed_comments = []

            fullnames = ['t1_' + comment['id'] for comment in comments]

            for comment in reddit.info(fullnames=fullnames):
                if comment.body == '[removed]':
                    removed_comments.append(comment.id)
                
            features = {
                'removed_comments': removed_comments,
                'num_removed': len(removed_comments),
                'requery_utc': int(time.time())
            }

            popular_threads.update_one({'id': id}, {'$set': features})

            break
        except Exception:
            print(f'Exception caught. Sleeping for a bit.')
            time.sleep(5)
            continue