'''
    To run this file, execute the following line:
        nohup python3 collect-pushshift.py > /dev/null 2>&1 &
    
    The line executes the script in the background and ignores
    any output it gets.

    The script will log information in its respective log file in /logs/.

    To find the process, execute the following line:
        ps aux | grep 'collect-popular.py'
'''

from psaw import PushshiftAPI
from pymongo import MongoClient
import datetime as dt
import os
import time
import sys


os.chdir('/shared/jackie/resilient-communities')

open('logs/collect-pushshift.log', 'w').close()


def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())


def log_message(message):
    with open('logs/collect-pushshift.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')


try:
    client = MongoClient('localhost', 27017)
    comments = client.pushshift_comments
    threads = client.pushshift_threads
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()

subreddit = 'PeopleFuckingDying'

api = PushshiftAPI()

# Collect comments.
if subreddit in comments.list_collection_names():
    start = int(next(comments[subreddit].find({})
                     .sort('created_utc', -1).limit(1))['created_utc'])
else:
    start = int(dt.datetime(2020, 1, 1).timestamp())

query = api.search_comments(
    subreddit=subreddit,
    sort_type='created_utc',
    sort='asc',
    after=start
)

for i, comment in enumerate(query):
    comments[subreddit].insert_one(comment.d_)

    if (i + 1) % 10_000 == 0:
        timestamp = dt.datetime.fromtimestamp(
            comment.created_utc).strftime('%x %I:%M:%S %p')
        log_message(f'r/{subreddit} comments @ {timestamp}.')

# Completed comments.
total_comments = comments[subreddit].estimated_document_count()

log_message(f'r/{subreddit} has {total_comments:,} comments.')

# Collect threads.
if subreddit in threads.list_collection_names():
    start = int(next(threads[subreddit].find({})
                     .sort('created_utc', -1).limit(1))['created_utc'])
else:
    start = int(dt.datetime(2020, 1, 1).timestamp())

query = api.search_submissions(
    subreddit=subreddit,
    sort_type='created_utc',
    sort='asc',
    after=start
)

for i, thread, in enumerate(query):
    threads[subreddit].insert_one(thread.d_)

    if (i + 1) % 10_000 == 0:
        timestamp = dt.datetime.fromtimestamp(
            threads.created_utc).strftime('%x %I:%M:%S %p')
        log_message(f'r/{subreddit} threads @ {timestamp}.')

total_threads = threads[subreddit].estimated_document_count()

log_message(f'r/{subreddit} has {total_threads:,} threads.')
log_message(f'r/{subreddit} has {total_comments:,} comments.')
