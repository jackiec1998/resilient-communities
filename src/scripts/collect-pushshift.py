'''
    To run this file, execute the following line:
        nohup python3 collect-pushshift.py > /dev/null 2>&1 &
    
    The line executes the script in the background and ignores
    any output it gets.

    The script will log information in its respective log file in /logs/.

    To find the process, execute the following line:
        ps aux | grep 'collect-popular.py'
'''

from psaw import PushshiftAPI as psaw
from pymongo import MongoClient
import datetime as dt
import os
import time
import sys
import warnings

warnings.filterwarnings('ignore')

os.chdir('/shared/jackie/resilient-communities')

with open('popular_subreddits.txt', 'r') as file:
    subreddits = file.read().split()

open('logs/collect-pushshift.log', 'w').close()
open('logs/pushshift-report.log', 'w').close()


def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())


def log_message(message):
    with open('logs/collect-pushshift.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')

def log_report(message):
    with open('logs/pushshift-report.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')


try:
    client = MongoClient('localhost', 27017)
    comments = client.resilient.pushshift_comments
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()

psaw = psaw()

def collect_comments(subreddit):
    while True:
        try:
            most_recent = comments.find_one({'subreddit': subreddit}, 
                {'created_utc': 1}, sort=[('created_utc', -1)])

            if most_recent == None:
                start = int(dt.datetime(2021, 6, 1).timestamp())
            else:
                start = most_recent


            query = psaw.search_comments(
                subreddit=subreddit,
                sort_type='created_utc',
                sort='asc',
                after=start
            )

            start = time.time()

            for i, comment in enumerate(query):
                comments.insert_one(comment.d_)

                if (i + 1) % 10_000 == 0:
                    timestamp = dt.datetime.fromtimestamp(
                        comment.created_utc).strftime('%x %I:%M:%S %p')

                    seconds_elapsed = time.time() - start

                    log_message(f'r/{subreddit} comments @ {timestamp}, {10_000 / seconds_elapsed:.2f} cps.')

                    start = time.time()

            break

        except Exception as e:
            log_message(e)
            continue

    log_report(f'r/{subreddit} comments completed.')


# def collect_threads(subreddit):
#     while True:
#         try:
#             if subreddit in threads.list_collection_names():
#                 start = int(next(threads[subreddit].find({})
#                                  .sort('created_utc', -1).limit(1))['created_utc'])
#             else:
#                 start = int(dt.datetime(2020, 1, 1).timestamp())

#             query = api.search_submissions(
#                 subreddit=subreddit,
#                 sort_type='created_utc',
#                 sort='asc',
#                 after=start
#             )

#             start = time.time()

#             for i, thread, in enumerate(query):
#                 threads[subreddit].insert_one(thread.d_)

#                 if (i + 1) % 10_000 == 0:
#                     timestamp = dt.datetime.fromtimestamp(
#                         thread.created_utc).strftime('%x %I:%M:%S %p')

#                     seconds_elapsed = time.time() - start
                    
#                     log_message(f'r/{subreddit} threads @ {timestamp}, {10_000 / seconds_elapsed:.2f} tps.')

#             break

#         except Exception as e:
#             log_message(e)
#             continue

#     log_report(f'r/{subreddit} threads completed.')


while True:

    for subreddit in subreddits:

        log_message(f'Collecting r/{subreddit} comments.')

        collect_comments(subreddit)

        # log_message(f'Collecting r/{subreddit} threads.')
        # collect_threads(subreddit)

    log_message('Sleeping for an hour.')
    log_report(f'Completed sweep.')
    time.sleep(3600)  # Wait an hour.
