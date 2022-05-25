from pymongo import MongoClient
import datetime as dt

started = dt.datetime.now()

print(f'Started:\t {started}')

client = MongoClient('localhost', 27017)
database = client.resilient.all

fields = [
    'approved_at_utc',
    'author_is_blocked',
    'comment_type',
    'mod_reason_by',
    'banned_by',
    'ups',
    'num_reports',
    'author_flair_type',
    'total_awards_received',
    'likes',
    'user_reports',
    'saved',
    'banned_at_utc',
    'mod_reason_title',
    'gilded',
    'archived',
    'collapsed_reason_code',
    'no_follow',
    'num_comments',
    'can_mod_post',
    'send_replies',
    'score',
    'report_reasons',
    'removal_reason',
    'approved_by',
    'controversiality',
    'top_awarded_type',
    'downs',
    'author_flair_css_class',
    'collapsed',
    'author_flair_richtext',
    'author_patreon_flair',
    'gildings',
    'collapsed_reason',
    'distinguished',
    'associated_award',
    'stickied',
    'author_premium',
    'can_gild',
    'unrepliable_reason',
    'author_flair_text_color',
    'subreddit_type',
    'author_flair_template_id',
    'subreddit_name_prefixed',
    'author_flair_text',
    'treatment_tags',
    'created',
    'awarders',
    'all_awardings',
    'locked',
    'author_flair_background_color',
    'collapsed_because_crowd_control',
    'mod_reports',
    'mod_note',
    'requeried',
    'requeried_utc',
    'removed'
]

# assert all(field in database.find_one({}).keys() for field in fields)

formatted_fields = {field: '' for field in fields}

results = database.update_many({}, {
    '$unset': formatted_fields
})

print(f'Finished:\t {dt.datetime.now()}')
print(f'Duration:\t {dt.datetime.now() - started}')
print(results.raw_result)