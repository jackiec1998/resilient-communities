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
import argparse

os.chdir('/shared/jackie/resilient-communities')

# parser = argparse.ArgumentParser(
#     description='What subreddits do you want to requery from?'
# )

# parser.add_argument(
#     '--subreddits',
#     nargs='+',
#     help='Give me the names of the subreddits you want to requery from.',
#     required=True
# )

# args = parser.parse_args()

# subreddits = args.subreddits

with open('sample.txt', 'r') as file:
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
            results = []

            message = f'Querying r/{subreddit} comments from database.'
            log_message(message)
            print(message)

            # Edit this to find created_utc's that are older than three days.
            three_days_ago = \
                int(time.time() - dt.timedelta(days=3).total_seconds())

            ids = pd.DataFrame(
                all_comments.find({'subreddit': {'$eq': subreddit},
                                   'created_utc': {'$lte': three_days_ago}}, {'id': 1})
            )['id']

            if os.path.exists(f'removed/{subreddit}.pkl'):
                df = pd.read_pickle(f'removed/{subreddit}.pkl')
            else:
                df = pd.DataFrame(columns=['id', 'retrieved_utc', 'removed'])

            fullnames = 't1_' + np.setdiff1d(ids, df['id'].values)

            message = f'Requerying {len(fullnames):,} r/{subreddit} comments.'
            log_message(message)
            print(message)

            for comment in tqdm(reddit.info(fullnames=fullnames.tolist())):
                results.append({
                    'id': comment.id,
                    'retrieved_utc': int(time.time()),
                    'removed': comment.body == '[removed]'
                })

            df = df.append(results, ignore_index=True)
            df.to_pickle(f'removed/{subreddit}.pkl')
            break

        except Exception as e:
            df = df.append(results, ignore_index=True)
            df.to_pickle(f'removed/{subreddit}.pkl')
            log_message(e)
            continue

        except KeyboardInterrupt:
            df = df.append(results, ignore_index=True)
            df.to_pickle(f'removed/{subreddit}.pkl')
            sys.exit()


reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT_ID_2'),
    client_secret=os.environ.get('REDDIT_CLIENT_SECRET_2'),
    user_agent=os.environ.get('REDDIT_USER_AGENT_2'),
    username=os.environ.get('REDDIT_USERNAME_2'),
    password=os.environ.get('REDDIT_PASSWORD_2')
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
