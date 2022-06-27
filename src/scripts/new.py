from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import datetime as dt
from psaw import PushshiftAPI as psaw
import time
import warnings
import praw
import os
import pickle
from dotenv import load_dotenv
import sys
import traceback

load_dotenv()

warnings.filterwarnings('ignore')

client = MongoClient('localhost', 27017)
popular_snapshots = client.resilient.popular
popular_threads = client.resilient.popular_threads
all_comments = client.resilient.all
newcomers_collection = client.resilient.newcomers

reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT_ID_2'),
    client_secret=os.environ.get('REDDIT_CLIENT_SECRET_2'),
    user_agent=os.environ.get('REDDIT_USER_AGENT_2'),
    username=os.environ.get('REDDIT_USERNAME_2'),
    password=os.environ.get('REDDIT_PASSWORD_2')
)

psaw = psaw()

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

def store_popular():

    print('Inserting and updating r/popular threads based on collected snapshots.')

    previous_count = popular_threads.estimated_document_count()

    threads = pd.DataFrame(
        popular_snapshots.aggregate([
            {'$sort': {'retrieved_utc': 1}},
            {'$group': {
                '_id': '$id',
                'subreddit': {'$first': '$subreddit'},
                'created_utc': {'$first': '$created_utc'},
                'title': {'$first': '$title'},
                'domain': {'$first': '$domain'},
                'permalink': {'$first': '$permalink'},
                'is_self': {'$first': '$is_self'},
                'author': {'$first': '$author'},
                'selftext': {'$first': '$selftext'},
                'is_video': {'$first': '$is_video'},
                'first_snapshot_utc': {'$min': '$retrieved_utc'},
                'last_snapshot_utc': {'$max': '$retrieved_utc'},
                'snapshots': {'$push': {
                    'retrieved_utc': '$retrieved_utc',
                    'rank': '$rank',
                    'score': '$score',
                    'upvote_ratio': '$upvote_ratio',
                    'num_comments': '$num_comments'
                }}
            }}
        ], allowDiskUse=True)
    ).rename(columns={'_id': 'id'}).set_index('id')

    for id, thread in tqdm(threads.iterrows(), total=len(threads)):
        features = thread.to_dict()

        features['created_to_popular'] = features['first_snapshot_utc'] - features['created_utc']
        features['popular_window'] = features['last_snapshot_utc'] - features['first_snapshot_utc']
        features['document_added_utc'] = int(time.time())

        popular_threads.update_one({'id': id}, {'$set': features, '$unset': {'base_document_modified_utc': ''}}, upsert=True)

    current_count = popular_threads.estimated_document_count()

    print(f'{current_count - previous_count:,} new r/popular threads.')

    return

def get_live_comments(thread_id):
    comments = pd.DataFrame(all_comments.find(
        {'link_id': 't3_' + thread_id},
        {'id': 1, 'body': 1, 'author': 1, 'parent_id': 1, 
            'permalink': 1, 'created_utc': 1, 'retrieved_utc': 1}
    ), columns=['_id', 'id', 'body', 'author', 'parent_id', 'permalink', 'created_utc', 'retrieved_utc'])

    if len(comments) == 0:
        return pd.DataFrame(columns=['id', 'body', 'author', 'parent_id',
            'permalink', 'created_utc', 'retrieved_utc', 'source']).set_index('id')
    
    comments = comments.drop(columns=['_id']).set_index('id')

    comments['source'] = 'live'

    return comments

def get_pushshift_comments(thread_id):
    fullname = 't3_' + thread_id

    comments = pd.DataFrame(psaw.search_comments(
        q = '*',
        link_id = fullname,
        filter = ['id', 'body', 'author', 'parent_id',
            'permalink', 'created_utc', 'retrieved_utc']
    )).drop(columns=['created', 'd_']).set_index('id')

    comments['source'] = 'psaw'

    return comments

def store_comments(thread_id):
    live_comments = get_live_comments(thread_id)

    # Pull comments from Pushshift.
    pushshift_comments = get_pushshift_comments(thread_id)

    # Take the ones with the lowest retrieved_utc.
    columns = ['body', 'author', 'parent_id', 
        'permalink', 'created_utc', 'retrieved_utc', 'source']

    complete_comments = pd.concat([
        live_comments[columns],
        pushshift_comments[columns]
    ]).reset_index().astype({'created_utc': 'float64', 'retrieved_utc': 'float64'})
    # Converting to float64 because there are some nan values that get
    # eliminated in the next line.

    complete_comments = complete_comments.iloc[
        complete_comments.groupby('id')['retrieved_utc'] \
            .idxmin().dropna().values
    ].set_index('id').sort_values('created_utc').astype({'created_utc': 'int64', 'retrieved_utc': 'int64'})


    # Insert them into the respective document.
    comment_dict = complete_comments.to_dict('index')

    popular_threads.update_one({'id': thread_id}, {
        '$set': {
            'comments': comment_dict,
            'num_comments': len(complete_comments),
            'authors': complete_comments['author'].unique().tolist(),
            'num_authors': int(complete_comments['author'].nunique()),
            'first_comment_utc': int(complete_comments['created_utc'].min()),
            'last_comment_utc': int(complete_comments['created_utc'].max()),
            'active_window': int(complete_comments['created_utc'].max() - \
                complete_comments['created_utc'].min()),
            'first_hundred': int(complete_comments.iloc[99]['created_utc'] - \
                list(popular_threads.find({'id': thread_id}, {'created_utc': 1})) \
                    [0]['created_utc']) if len(complete_comments) >= 100 else None,
            'retrieved_comments_utc': int(time.time())
        },
        '$unset': {
            'praw_utc': '',
            'first_utc': '',
            'last_utc': '',
            'complete_utc': '',
            'first_hundred_utc': ''
        }
    })

    return complete_comments.index.to_list(), complete_comments['author'].unique().tolist()

def flag_removed_comments(thread_id, comment_ids, disable):

    attempts = 1
    removed_comments = []

    while True:
        try:
            fullnames = ['t1_' + id for id in comment_ids]

            for comment in tqdm(reddit.info(fullnames=fullnames), total=len(fullnames), disable=disable):
                if comment.body == '[removed]':
                    removed_comments.append(comment.id)

            features = {
                'removed_comments': removed_comments,
                'num_removed': len(removed_comments),
                'requeried_comments_utc': int(time.time())
            }

            popular_threads.update_one({'id': thread_id}, {'$set': features,
                '$unset': {'requeried_utc': ''}})

            break

        except Exception as e:
            if attempts >= 5:
                print(e)
                break
            attempts += 1

    return

def flag_newcomers(thread_id, authors, memoize, disable):

    fullname = 't3_' + thread_id

    subreddit = get_popular_threads(
        filter = {'id': thread_id}, 
        columns = ['subreddit']
    )['subreddit'].values[0]

    newcomers = []
    troublemakers = []

    for author in tqdm(authors, total=len(authors), disable=disable):

        if (author, subreddit) not in memoize:
            try:
                first_fullname = next(psaw.search_comments(
                    author = author,
                    subreddit = subreddit,
                    filter = ['link_id'],
                    limit = 1,
                    sort = 'asc'
                )).link_id
            except Exception as e:
                troublemakers.append(author)
                continue

            memoize[(author, subreddit)] = first_fullname

        else:
            first_fullname = memoize[(author, subreddit)]

        # They're a newcomer.
        if first_fullname == fullname:

            newcomers.append(author)

            # Collect their user history.
            comments = pd.DataFrame(psaw.search_comments(
                author = author,
                subreddit = subreddit,
                filter = ['id', 'body', 'author', 'parent_id',
                    'permalink', 'created_utc', 'retrieved_utc']
            )).drop(columns=['created']).set_index('id')

            comments = comments.sort_values(by='created_utc')

            comments_dict = comments.to_dict('index')

            newcomers_collection.update_one({'author': author, 'subreddit': subreddit}, {
                '$set': {
                    'author': author,
                    'subreddit': subreddit,
                    'comments': comments_dict,
                    'num_comments': len(comments),
                    'joined_at_utc': int(comments['created_utc'].min()),
                    'last_seen_utc': int(comments['created_utc'].max()),
                    'tenure': int(comments['created_utc'].max() - comments['created_utc'].min()),
                    'last_updated_utc': int(time.time())
                }
            })

    popular_threads.update_one({'id': thread_id}, {
        '$set': {
            'newcomers': newcomers,
            'num_newcomers': len(newcomers),
            'troublemakers': troublemakers,
            'num_troublemakers': len(troublemakers),
            'flagged_newcomers_utc': int(time.time())
        }
    })

    return memoize

def generate_features():

    thread_ids = get_popular_threads(
        filter = {
            '$or': [
                {'retrieved_comments_utc': None},
                {'requeried_comments_utc': None}
            ]
        }
    ).index.to_list()

    if os.path.isfile('missed_ids.pkl'):
        with open('missed-ids.pkl', 'rb') as file:
            missed_ids = pickle.load(file)
    else:
        missed_ids = []
    # memoize = {}
    completed = 0
    disable = False

    for thread_id in tqdm(thread_ids, total=len(thread_ids), disable=disable):

        attempts = 1

        while True:
            try:
                if disable:
                    print('Storing comments.')
                comment_ids, authors = store_comments(thread_id)

                # Find the flag the removed comments.
                if disable:
                    print('Flagging removed comments.')
                flag_removed_comments(thread_id, comment_ids, not disable)

                # Flag the authors that are new.
                # if disable:
                    # print('Flagging newcomers.')
                # flag_newcomers(thread_id, authors, memoize, False)

                break

            except KeyboardInterrupt:
                print('Detected keyboard interruption.')
                print(f'Missed IDs: {missed_ids}')

                with open('missed-ids.pkl', 'wb') as file:
                    pickle.dump(missed_ids, file)

                sys.exit()

            except Exception as e:
                if attempts >= 10:
                    print(e)
                    missed_ids.add(thread_id)
                    continue

                print(e)
                traceback.print_exc()
                attempts += 1

        if disable:
            completed += 1
            print(f'{completed:,} / {len(thread_ids)} threads completed.')

if __name__ == '__main__':

    start = int(time.time())

    # Store r/popular threads and snapshots.
    store_popular()

    # Store r/popular thread comments, find removed comments, find newcomers.
    # I.e., generate the features.
    generate_features()

    print(f'Script completed @ {dt.timedelta(seconds=int(time.time()) - start)}')

