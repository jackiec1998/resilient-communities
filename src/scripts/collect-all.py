'''
    To run this file, execute the following line:
        nohup python3 collect-all.py > /dev/null 2>&1 &

    The line executes the script in the background and ignores
    any output it gets.

    The script will log information in its respective log file
    in /logs/.

    To find the process, execute the following line:
        ps aux | grep 'collect-all.py'
'''
import praw
import time
import sys
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import date
from slack_sdk import WebClient

load_dotenv()

os.chdir('../../') 

open('logs/collect-all.log', 'w').close()

def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())

def post_message(message, channel='logging'):
    slack.chat_postMessage(
        channel = channel,
        text = message
    )

def log_message(message):
    with open('logs/collect-all.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')

UPDATE_INTERVAL = 100_000

# Connecting to PRAW.
reddit = praw.Reddit(
    client_id = os.environ.get('REDDIT_CLIENT_ID'),
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET'),
    user_agent = os.environ.get('REDDIT_USER_AGENT'),
    username = os.environ.get('REDDIT_USERNAME'),
    password = os.environ.get('REDDIT_PASSWORD')
)

if reddit is None or reddit.read_only:
    log_message('PRAW was not instantiated correctly. Exiting.')
    sys.exit()

# Connecting to Slack.
try:
    slack = WebClient(token=os.environ.get('SLACK_BOT_TOKEN'))
    post_message('r/all collection connected to Slack.')
except Exception as e:
    log_message('Slack was not instantiated correctly. Exiting.')
    sys.exit()

# Connecting to MongoDB.
try:
    client = MongoClient('localhost', 27017)
    collection = client.resilient.all
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()

LAST_SLACK_UPDATE = None

while True:
    
    try:
        start = time.time()

        comments = []

        stream = reddit.subreddit('all').stream.comments(skip_existing=True)

        for i, comment in enumerate(stream):
            comment = {key: value for key, value in vars(comment).items() \
                if not key.startswith('_')}

            comment.update({
                'author': comment['author'].name \
                    if comment['author'] is not None else '[deleted]',
                'subreddit': comment['subreddit'].display_name \
                    if comment['subreddit'] is not None else '',
                'retrieved_utc': int(time.time()),
                'requeried': False,
                'requeried_utc': 0,
                'removed': comment['body'] == '[removed]'
            })

            comments.append(comment)

            if (i + 1) % UPDATE_INTERVAL == 0:
                seconds_elapsed = time.time() - start

                collection.insert_many(comments)

                num_comments = collection.estimated_document_count()

                log_message(f'STATUS UPDATE | {num_comments:,} r/all comments | '\
                    f'{UPDATE_INTERVAL / seconds_elapsed:.2f} cps')

                comments = []

                start = time.time()

                if date.today().strftime('%x %p') != LAST_SLACK_UPDATE:
                    post_message('r/popular comment report: ' \
                        f'There are currently {num_comments:,} r/popular comments in the database.') 

                    LAST_SLACK_UPDATE = date.today().strftime('%x %p')

    except praw.exceptions.RedditAPIException as e:
        log_message('PRAW exception caught.')
        log_message(e)

        time.sleep(300) # Sleep for five minutes.

        continue

    except Exception as e:
        log_message('Unhandled exception caught.')
        log_message(e)

        time.sleep(300) # Sleep for five minutes.

        continue