import pandas as pd
from pymongo import MongoClient
import praw
from prawcore.exceptions import Forbidden
from psaw import PushshiftAPI as psaw
from tqdm import tqdm
import os
import warnings
import time
import sys
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings('ignore')

reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT_ID_2'),
    client_secret=os.environ.get('REDDIT_CLIENT_SECRET_2'),
    user_agent=os.environ.get('REDDIT_USER_AGENT_2'),
    username=os.environ.get('REDDIT_USERNAME_2'),
    password=os.environ.get('REDDIT_PASSWORD_2')
)

psaw = psaw()

client = MongoClient('localhost', 27017)
all_comments = client.resilient.all
popular_threads = client.resilient.popular_threads

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

def get_live_comments(thread_id):
    comments = pd.DataFrame(all_comments.find(
        {'link_id': 't3_' + thread_id},
        {'id': 1, 'body': 1, 'author': 1, 'parent_id': 1, 'permalink': 1, 'created_utc': 1, 'retrieved_utc': 1}
    ))

    if len(comments) == 0:
        return pd.DataFrame()

    comments = comments.drop(columns=['_id']).set_index('id')

    comments['source'] = 'LIVE'

    return comments

def get_pushshift_comments(thread_id):
    comments = pd.DataFrame(psaw.search_comments(
        q = '*',
        link_id = 't3_' + thread_id,
        filter = ['id', 'body', 'author', 'parent_id', 'permalink', 'created_utc', 'retrieved_utc']
    )).drop(columns=['created', 'd_']).set_index('id')

    comments['source'] = 'PSAW'

    return comments

def get_praw_comments(thread_id):
    try:
        submission = reddit.submission(id=thread_id)
        submission.comments.replace_more(limit=None)


        comments = pd.DataFrame([{
            'id': comment.id,
            'body': comment.body,
            'author': comment.author.name \
                if comment.author is not None else '[deleted]',
            'parent_id': comment.parent_id,
            'created_utc': comment.created_utc,
            'permalink': comment.permalink
        } for comment in submission.comments.list()]).set_index('id')

        comments['retrieved_utc'] = int(time.time())
        comments['source'] = 'PRAW'

        return comments
    except Forbidden:
        return pd.DataFrame()

# thread_ids = get_popular_threads(filter={'num_comments': {'$lte': 100}}).index.to_list()
thread_ids = ['tkwftp']
missed_ids = []


for thread_id in tqdm(thread_ids):

    attempts = 1

    while True:
        try:
            live_comments = get_live_comments(thread_id)
            pushshift_comments = get_pushshift_comments(thread_id)
            praw_comments = get_praw_comments(thread_id)

            complete_comments = pd.concat([
                live_comments,
                pushshift_comments,
                praw_comments
            ]).reset_index()

            complete_comments = complete_comments.iloc[
                complete_comments.groupby('id')['retrieved_utc'] \
                    .idxmin().dropna().values
            ].set_index('id').sort_values('created_utc')

            comment_dict = complete_comments.to_dict('index')
            
            popular_threads.update_one({'id': thread_id}, {
                '$set': {
                    'comments': comment_dict,
                    'num_comments': len(comment_dict),
                    'authors': complete_comments['author'].unique().tolist(),
                    'num_authors': complete_comments['author'].nunique(),
                    'first_comment_utc': complete_comments['created_utc'].min(),
                    'last_comment_utc': complete_comments['created_utc'].max(),
                    'active_window': complete_comments['created_utc'].max() - \
                        complete_comments['created_utc'].min(),
                    'complete_utc': int(time.time()),
                    'first_hundred': complete_comments.iloc[99]['created_utc'] - \
                        list(popular_threads.find({'id': thread_id}, {'created_utc': 1}))[0]['created_utc']
                },
                '$unset': {
                    'praw_utc': '',
                    'first_utc': '',
                    'last_utc': ''
                }
            })
            break

        except KeyboardInterrupt:
            print('Keyboard interrupt.')
            print(missed_ids)
            sys.exit()

        except Exception as e:

            print(e)

            if attempts >= 10:
                missed_ids.append(thread_id)
                break
            else:
                attempts += 1

            continue

        
            
