'''
    To run this file, execute the following line:
        nohup python3 collect-popular.py > /dev/null 2>&1 &

    The line executes the script in the background and ignores
    any output it gets.

    The script will log information in its respective log file
    in /logs/.

    To find the process, execute the following line:
        ps aux | grep 'collect-popular.py'
'''
import praw
import time
import os
import sys
from slack_sdk import WebClient
import datetime as dt
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

os.chdir('/shared/jackie/resilient-communities/') 

open('logs/collect-popular.log', 'w').close()

def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())

def post_message(message, channel='logging'):
    slack.chat_postMessage(
        channel = channel,
        text = message
    )

def log_message(message):
    with open('logs/collect-popular.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')

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
    post_message('r/popular collection connected to Slack.')
except Exception as e:
    log_message('Slack was not instantiated correctly. Exiting.')
    sys.exit()

# Connecting to MongoDB.
try:
    client = MongoClient('localhost', 27017)
    collection = client.resilient.popular
except Exception as e:
    log_message('Database did not connect successfully.')
    sys.exit()

# Data type sanity check.
properties = [
    'comment_limit',
    'approved_at_utc',
    'subreddit',
    'selftext',
    'author_fullname',
    'mod_reason_title',
    'gilded',
    'title',
    'subreddit_name_prefixed',
    'hidden',
    'downs',
    'hide_score',
    'name',
    'quarantine',
    'upvote_ratio',
    'subreddit_type',
    'ups',
    'total_awards_received',
    'is_original_content',
    'user_reports',
    'is_reddit_media_domain',
    'is_meta',
    'category',
    'link_flair_text',
    'can_mod_post',
    'score',
    'approved_by',
    'author_premium',
    'edited',
    'is_self',
    'mod_note',
    'created',
    'removed_by_category',
    'banned_by',
    'domain',
    'allow_live_comments',
    'likes',
    'suggested_sort',
    'banned_at_utc',
    'url_overridden_by_dest',
    'view_count',
    'archived',
    'is_crosspostable',
    'pinned',
    'over_18',
    'media_only',
    'can_gild',
    'spoiler',
    'locked',
    'author_flair_text',
    'removed_by',
    'num_reports',
    'distinguished',
    'subreddit_id',
    'mod_reason_by',
    'removal_reason',
    'id',
    'is_robot_indexable',
    'author',
    'discussion_type',
    'num_comments',
    'whitelist_status',
    'contest_mode',
    'mod_reports',
    'permalink',
    'parent_whitelist_status',
    'stickied',
    'url',
    'subreddit_subscribers',
    'created_utc',
    'num_crossposts',
    'media',
    'is_video'
]

thread_properties = vars(next(reddit.subreddit('popular').hot(limit=1))).keys()
invalid_properties = []
for property in properties:

    if property is 'url_overridden_by_dest':
        continue

    if property not in thread_properties:
        invalid_properties.append(property)

if len(invalid_properties) > 0:
    log_message(f'Found invalid properties in set: {invalid_properties}')
    sys.exit()

last_slack_update = dt.datetime.now() - dt.timedelta(hours=6)
UPDATE_INTERVAL = dt.timedelta(hours=6)
num_requests = 0
LOG_INTERVAL = 10

while True:
    try:
        threads = [vars(thread) for thread in reddit.subreddit('popular').hot(limit=100)]
        
        threads = [{key: value for key, value in thread.items() if key in properties} \
            for thread in threads]

        for rank, thread in enumerate(threads):
            thread['subreddit'] = thread['subreddit'].display_name \
                if thread['subreddit'] is not None else ''

            thread['author'] = thread['author'].name \
                if thread['author'] is not None else '[deleted]'

            thread['rank'] = rank

            thread['retrieved_utc'] = int(time.time())

        collection.insert_many(threads)

        time.sleep(120)

        num_requests += 1

        if num_requests % LOG_INTERVAL == 0:

            num_threads = len(collection.distinct('id'))
            num_documents = collection.estimated_document_count()
            num_subreddits = len(collection.distinct('subreddit'))

            log_message(f'{num_requests:,} requests | {num_documents:,} documents | ' \
                f'{num_threads:,} threads | {num_subreddits:,} subreddits')

            time_since_last_update = dt.datetime.now() - last_slack_update

            if time_since_last_update >= UPDATE_INTERVAL:
                post_message('*r/popular thread report:* ' \
                    f'There are currently {num_threads:,} r/popular threads and ' \
                    f'{num_documents:,} snapshots from ' \
                    f'{num_subreddits:,} subreddits in the database.')

                last_slack_update = dt.datetime.now()

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