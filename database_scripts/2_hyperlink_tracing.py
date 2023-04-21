from bs4 import BeautifulSoup
import json
import os
import praw
import prawcore
import psycopg
#from psycopg import extras as ext
import requests
import os
from time import sleep
import re


"""
This script "snowballs" in two steps:

While there are still unprocessed subreddits in the processing queue:

    1. Metadata Step: pull metadata for all the subreddits in the processing
    queue (or, if the subreddit is private/no longer exists, mark it
    unsuccessful)

    2. Snowball Step: extract the hyperlink ties to other subreddits out of the
    metadata text fields using a regular expression, add the ties to the
    database, and add any newly discovered subreddits to the processing queue.
    Mark the processed subreddits as complete or unsuccessful in the processing
    queue.


Generally, 0 = unprocessed, 1 = processed, and -1 = unsuccessful


To run:

1) Replace `reddit_config_path` with the path to a text file in the following format:

user_agent=NAME_OF_USER_AGENT
client_id=CLIENT_ID
client_secret=CLIENT_SECRET
username=USERNAME
password=PASSWORD


2) Replace `db_config_path` with the path to a text file in the following format:

host='database_host'
dbname='database_name'
user='username'
password='password'


3) Make sure a table exists to store metadata, as well as the processing queue
and edges/ties

4) Add the seed subreddits to the processing queue with processed = 0 to start the snowball

"""

reddit_config_path = "/Users/lgs17/Desktop/Reddit Collab 2022/code/reddit_config.txt"

db_config_path = "/Users/lgs17/Desktop/Reddit Collab 2022/code/db_config.txt"

##########################
## Reddit API functions ##
##########################


## Authenticate PRAW API object
def init_reddit():
    print("Creating Reddit object...", flush=True)
    reddit_config = {}
    with open(reddit_config_path) as f:
        for line in f.readlines():
            key, value = line.split("=")
            reddit_config[key.strip()] = value.strip()
        reddit = praw.Reddit(**reddit_config)
        return reddit


## Scrape metadata for a given subreddit name
def scrape_subreddit_metadata(reddit, subreddit_name):
    print("\tScraping {}...".format(subreddit_name), flush=True)

    ## Retrieve a PRAW subreddit object for the given subreddit name
    def get_subreddit(reddit, subreddit_name):
        return reddit.subreddit(subreddit_name)

    ## Pull JSON data for a post's author and cache it
    def pull_author(author):
        if author:
            try:
                _ = author.created_utc ## To force the lazy object to load

            except (prawcore.exceptions.NotFound, AttributeError) as ex:
                return

            return author.name

    subreddit = get_subreddit(reddit, subreddit_name)

    try:
        _ = subreddit.description ## Force lazy object to load
    except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden, prawcore.exceptions.Redirect, prawcore.exceptions.BadRequest, AttributeError) as ex:
        return

    moderators = json.dumps([pull_author(moderator) for moderator in subreddit.moderator()])

    subreddit_rules = json.dumps(subreddit.rules().get("rules"))

    row = [subreddit_name] + [str(subreddit.__dict__.get(key)) for key in ["display_name", "free_form_reports", \
    "subreddit_type", "community_icon", "banner_background_image", "header_title", "over18", \
    "show_media", "description", "title", "collapse_deleted_comments", "id", "emojis_enabled", \
    "can_assign_user_flair", "allow_videos", "spoilers_enabled", "active_user_count", \
    "original_content_tag_enabled", "display_name_prefixed", "can_assign_link_flair", \
    "submit_text", "allow_videogifs", "accounts_active", "public_traffic", "subscribers", \
    "all_original_content", "lang", "has_menu_widget", "name", "user_flair_enabled_in_sr", \
    "created", "url", "quarantine", "hide_ads", "created_utc", "allow_discovery", "accounts_active_is_fuzzed", \
    "advertiser_category", "public_description", "link_flair_enabled", "allow_images", "videostream_links_count", \
    "comment_score_hide_mins", "show_media_preview", "submission_type"]] + [moderators, subreddit_rules]

    for index in range(len(row)):
        if row[index] == "None":
            row[index] = None

    return row


########################
## Database functions ##
########################


## Get authenticated conn and cursor objects (returns conn, cursor tuple)
def get_db():
    with open(db_config_path) as f:
        conn_str = " ".join([l.strip() for l in f.readlines()])
    conn = psycopg.connect(conn_str)
    cursor = conn.cursor()
    return (conn, cursor)


## Execute single query and return results
def execute_in_db(query, return_results = False, return_first_only = False, args = None, batch_insert = False):
    conn, cursor = get_db()

    if args and batch_insert:
        for row in args:
            cursor.execute(query, row)
    elif args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)

    if return_first_only:
        results = [row[0] for row in cursor.fetchall()]
    elif return_results:
        results = cursor.fetchall()
    else:
        results = None

    conn.commit()

    cursor.close()
    conn.close()

    return results


## Insert a row of subreddit metadata into the database
def insert_subreddit_metadata_row(metadata_row, subreddit):
    insert_successful_q = """ UPDATE t2_subreddit_metadata SET display_name = %s, free_form_reports = %s, subreddit_type = %s, community_icon = %s, banner_background_image = %s, header_title = %s, over18 = %s, show_media = %s, description = %s, title = %s, collapse_deleted_comments = %s, subreddit_id = %s, emojis_enabled = %s, can_assign_user_flair = %s, allow_videos = %s, spoilers_enabled = %s, active_user_count = %s, original_content_tag_enabled = %s, display_name_prefixed = %s, can_assign_link_flair = %s, submit_text = %s, allow_videogifs = %s, accounts_active = %s, public_traffic = %s, subscribers = %s, all_original_content = %s, lang = %s, has_menu_widget = %s, name = %s, user_flair_enabled_in_sr = %s, created = %s, url = %s, quarantine = %s, hide_ads = %s, created_utc = %s, allow_discovery = %s, accounts_active_is_fuzzed = %s, advertiser_category = %s, public_description = %s, link_flair_enabled = %s, allow_images = %s, videostream_links_count = %s, comment_score_hide_mins = %s, show_media_preview = %s, submission_type = %s, moderators = %s, rules = %s, has_metadata = 1 WHERE subreddit = %s """

    mark_unsuccessful_q = """ UPDATE t2_subreddit_metadata SET has_metadata = -1 WHERE subreddit = %s """

    if metadata_row:
        execute_in_db(insert_successful_q, args = metadata_row[1:] + [subreddit])
    else:
        execute_in_db(mark_unsuccessful_q, args = [subreddit])


## Update metadata table and processing queue
def update_subreddit_metadata_table(edges_found, queue_table, step):
    select_in_metadata_q = """ SELECT subreddit FROM t2_subreddit_metadata """
    add_to_metadata_q = """ INSERT INTO t2_subreddit_metadata (subreddit, has_metadata) VALUES (%s, 0) """
    select_in_queue_q = """ SELECT subreddit FROM {} """.format(queue_table)
    add_to_queue_q = """ INSERT INTO {} (subreddit, step) VALUES (%s, %s) """.format(queue_table, step)

    subreddits_in_edgelist = set([edge[1] for edge in edges_found if edge[1]])

    ## add subreddits to metadata table if they're not there already
    in_metatadata_table = set(execute_in_db(select_in_metadata_q, return_first_only = True))
    subreddits_to_add = subreddits_in_edgelist.difference(in_metatadata_table)
    if subreddits_to_add:
        metadata_rows = [(subreddit.lower(),) for subreddit in subreddits_to_add]
        execute_in_db(add_to_metadata_q, args = metadata_rows, batch_insert = True)

    ## add to processing queue if they're not there already
    in_queue = set(execute_in_db(select_in_queue_q, return_first_only = True))
    subreddits_to_process = subreddits_in_edgelist.difference(in_queue)
    if subreddits_to_process:
        queue_rows = [(subreddit, step) for subreddit in subreddits_to_process]
        execute_in_db(add_to_queue_q, args = queue_rows, batch_insert = True)


############################
## Link tracing functions ##
############################

## Return list
def trace_ties(subreddit, regexp):
    get_metadata_q = """ SELECT complete_metadata_text FROM t2_subreddit_metadata WHERE subreddit = '{}' """.format(subreddit)

    subreddit_metadata_text = execute_in_db(get_metadata_q, return_first_only = True)[0]

    outgoing_hyperlinks = [link[-1].lower() for link in re.findall(regexp, subreddit_metadata_text)]

    outgoing_hyperlinks = [link for link in outgoing_hyperlinks if link != subreddit]

    edgelist = [(subreddit, link, None) for link in outgoing_hyperlinks]

    return(edgelist)


######################
## Running snowball ##
######################

## Metadata Step: check if any subreddits need to have their metadata collected
## (i.e. if they are marked with has_metadata = 0 in the metadata table)
def subreddit_metadata_step(reddit, queue_table):
    select_q = """ SELECT subreddit FROM t2_subreddit_metadata WHERE has_metadata = 0 AND subreddit IN (SELECT subreddit FROM {})""".format(queue_table)

    ## get list of subreddits without metadata (`has_metadata` = 0)
    queue = execute_in_db(select_q, return_first_only = True)
    print("{} subreddits to pull metadata".format(len(queue)), flush = True)

    ## pull metadata for them
    while queue:
        next_subreddit = queue.pop()
        metadata_row = scrape_subreddit_metadata(reddit, next_subreddit)
        insert_subreddit_metadata_row(metadata_row, subreddit=next_subreddit)

    ## Generate complete text (combined text field) from new metadata fields
    generate_complete_text_q = """ UPDATE t2_subreddit_metadata SET complete_metadata_text = CONCAT_WS(' ',
        REGEXP_REPLACE(REPLACE(display_name, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(header_title, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(description, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(title, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(submit_text, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(name, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(public_description, '"', ''), E'[\\n\\r]+', ' ', 'g' ),
        REGEXP_REPLACE(REPLACE(rules::TEXT, '"', ''), E'[\\n\\r]+', ' ', 'g' ))
        WHERE complete_metadata_text IS NULL AND has_metadata = 1 """

    execute_in_db(generate_complete_text_q)

## Snowball Step: scrape edges and add them to the database; if there are any
## new subreddits in the scraped edges, add them to the processing queue
def snowball_step(edges_table, queue_table, trace_regexp):
    mark_no_metadata_q = """ UPDATE {} SET processed = -1 FROM t2_subreddit_metadata WHERE {}.subreddit = t2_subreddit_metadata.subreddit AND processed = 0 AND has_metadata = -1 """.format(queue_table, queue_table)
    select_subreddits_to_process_q = """ SELECT subreddit, step FROM {} WHERE processed = 0 ORDER BY step, subreddit """.format(queue_table)
    insert_edges_q = """ INSERT INTO {} (source, target, label) VALUES (%s, %s, %s) """.format(edges_table)
    set_processed_q = """ UPDATE {} SET processed = 1 WHERE subreddit = %s """.format(queue_table)

    ## mark the subreddits that can't be processed (no metadata)
    execute_in_db(mark_no_metadata_q)

    ## get list of (subreddit, step) of unprocessed subreddits (`processed` = 0)
    queue = execute_in_db(select_subreddits_to_process_q, return_results = True)
    print("{} subreddits to process ties".format(len(queue)), flush = True)
    update_processed_rows = []

    ## scrape edges and add to edges table (source_subreddit,
    ## target_subreddit, label)
    while queue:
        ## get next subreddit to process
        next_subreddit, next_subreddit_step = queue.pop()
        edges_to_upload = []

        ## process
        edges_found = trace_ties(next_subreddit, trace_regexp)
        if edges_found:
            edges_found = list(set(edges_found)) ## get rid of duplicates
            edges_to_upload.extend(edges_found)

            ## add new subreddits to metadata table
            update_subreddit_metadata_table(edges_found, queue_table, next_subreddit_step + 1)
            ## add edges to database
            execute_in_db(insert_edges_q, args = edges_to_upload, batch_insert = True)

        update_processed_rows.append((next_subreddit,))

    print("{} rows processed".format(len(update_processed_rows)), flush = True)
    ## mark processed subreddits in metadata table
    if update_processed_rows:
        execute_in_db(set_processed_q, args = update_processed_rows, batch_insert = True)


## Function to see if there are still unprocessed subreddits in the queue
def check_num_unprocessed(queue_table):
    unprocessed_count_q = """ SELECT COUNT(*) FROM {} WHERE processed = 0 """.format(queue_table)

    unprocessed_count = execute_in_db(unprocessed_count_q, return_first_only = True)[0]

    return unprocessed_count

## Get an authenticated Reddit API object
reddit = init_reddit()

## Scrape hyperlink edges
hyperlink_queue, hyperlink_edges = "t1a_hyperlink_queue", "t1a_hyperlink_ties"
link_exp = "(reddit.com)/r/([A-Za-z0-9_-]+)"

while check_num_unprocessed(queue_table=hyperlink_queue) > 0:
    print("Snowballing...", flush=True)
    subreddit_metadata_step(reddit, hyperlink_queue)
    snowball_step(edges_table=hyperlink_edges, queue_table=hyperlink_queue, trace_regexp=link_exp)
print("Hyperlinks done.")

## Scrape reference edges
ref_queue, ref_edges = "t1b_reference_queue", "t1b_reference_ties"
ref_exp = "(((^|\s)(/)?)|(reddit.com/))r/([A-Za-z0-9_-]+)"
while check_num_unprocessed(queue_table=ref_queue) > 0:
    subreddit_metadata_step(reddit, ref_queue)
    snowball_step(edges_table=ref_edges, queue_table=ref_queue, trace_regexp=ref_exp)
print("References done.")
