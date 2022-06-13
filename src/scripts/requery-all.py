from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
import numpy as np
import datetime as dt
import praw
import sys
import os
import time

os.chdir('/shared/jackie/resilient-communities')

with open('popular_subreddits.txt', 'r') as file:
    subreddits = file.read().split()

load_dotenv()

open('logs/requery-all.log', 'w').close()

def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())

def log_message(message):
    with open('logs/requery-all.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')


try:
    client = MongoClient('localhost', 27017)
    all_comments = client.resilient.all
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()

def requery_subreddit(subreddit):
    while True:
        try:

            message = f'Querying r/{subreddit} comments from database.'
            log_message(message)
            print(message)

            # Edit this to find created_utc's that are older than three days.
            three_days_ago = \
                int(time.time() - dt.timedelta(days=3).total_seconds())

            ids = pd.DataFrame(
                all_comments.find({
                    'subreddit': subreddit,
                    'requeried_utc': 0,
                    'created_utc': {'$lte': three_days_ago}
                }, {'id': 1})
            )

            if len(ids) == 0:
                log_message(f'Nothing to update for r/{subreddit}.')
                print(f'Nothing to update for r/{subreddit}.')
                break

            fullnames = 't1_' + ids['id'].values

            message = f'Requerying {len(fullnames):,} r/{subreddit} comments.'
            log_message(message)
            print(message)

            for comment in tqdm(reddit.info(fullnames=fullnames.tolist()), total=len(fullnames.tolist())):

                all_comments.update_one({'id': comment.id}, {'$set': {
                    'requeried_utc': int(time.time()),
                    'removed': comment.body == '[removed]'
                }})

            break

        except Exception as e:
            log_message(e)
            continue

        except KeyboardInterrupt:
            sys.exit()


reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT_ID_3'),
    client_secret=os.environ.get('REDDIT_CLIENT_SECRET_3'),
    user_agent=os.environ.get('REDDIT_USER_AGENT_3'),
    username=os.environ.get('REDDIT_USERNAME_3'),
    password=os.environ.get('REDDIT_PASSWORD_3')
)

if reddit is None or reddit.read_only:
    log_message('PRAW was not instantiated correctly. Exiting.')
    sys.exit()

try:
    client = MongoClient('localhost', 27017)
    collection = client.resilient.all
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()


while True:
    for subreddit in subreddits:

        requery_subreddit(subreddit)

    log_message('Sleeping for one hour.')
    time.sleep(3600)
