from bs4 import BeautifulSoup
import json
import os
import praw
import prawcore
import psycopg
import os
from time import sleep
import re

##########################
## Reddit API functions ##
##########################

## Log in to Reddit API
def init_reddit():
    print("Creating Reddit object...", flush=True)
    reddit_config = {}
    with open("../../reddit_config.txt") as f:
        for line in f.readlines():
            key, value = line.split("=")
            reddit_config[key.strip()] = value.strip()
        reddit = praw.Reddit(**reddit_config)
        return reddit

## Scrape the metadata for a given subreddit using PRAW, including the subreddit's moderators
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
    except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden, prawcore.exceptions.Redirect, prawcore.exceptions.BadRequest, prawcore.exceptions.ServerError, AttributeError) as ex:
        return
    
    try:
        moderators = json.dumps([pull_author(moderator) for moderator in subreddit.moderator()])
    except prawcore.exceptions.ServerError as ex:
        return
    
    subreddit_rules = []
    for rule in subreddit.rules:
        subreddit_rules.append(str(rule))
    subreddit_rules = json.dumps(subreddit_rules)
        
#    print("RULES: {}\n\n".format(", ".join(subreddit_rules)), flush=True)

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


## Scrape the list of subreddits moderated by a given user
def scrape_moderator_roles(reddit, username):
    moderated_subreddits = []
    
    ## Make sure that user exists (i.e. check that account corresponding to username exists)
    try:
        reddit_user = reddit.redditor(username)
        moderated = reddit_user.moderated()
    except (prawcore.exceptions.NotFound, AttributeError) as ex:
            return
    
    for subreddit in moderated:
        moderated_subreddits.append(str(subreddit).lower())
    
    return moderated_subreddits




########################
## Database functions ##
########################


## Get authenticated conn and cursor objects (returns conn, cursor tuple)
def get_db():
    with open("../../db_config.txt") as f:
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


def insert_subreddit_metadata_row(metadata_row, subreddit):
    insert_successful_q = """ UPDATE t2_subreddit_metadata SET display_name = %s, free_form_reports = %s, subreddit_type = %s, community_icon = %s, banner_background_image = %s, header_title = %s, over18 = %s, show_media = %s, description = %s, title = %s, collapse_deleted_comments = %s, subreddit_id = %s, emojis_enabled = %s, can_assign_user_flair = %s, allow_videos = %s, spoilers_enabled = %s, active_user_count = %s, original_content_tag_enabled = %s, display_name_prefixed = %s, can_assign_link_flair = %s, submit_text = %s, allow_videogifs = %s, accounts_active = %s, public_traffic = %s, subscribers = %s, all_original_content = %s, lang = %s, has_menu_widget = %s, name = %s, user_flair_enabled_in_sr = %s, created = %s, url = %s, quarantine = %s, hide_ads = %s, created_utc = %s, allow_discovery = %s, accounts_active_is_fuzzed = %s, advertiser_category = %s, public_description = %s, link_flair_enabled = %s, allow_images = %s, videostream_links_count = %s, comment_score_hide_mins = %s, show_media_preview = %s, submission_type = %s, moderators = %s, rules = %s, has_metadata = 1 WHERE subreddit = %s """

    mark_unsuccessful_q = """ UPDATE t2_subreddit_metadata SET has_metadata = -1 WHERE subreddit = %s """
    
    if metadata_row:
        execute_in_db(insert_successful_q, args = metadata_row[1:] + [subreddit])
    else:
        execute_in_db(mark_unsuccessful_q, args = [subreddit])

    ## Generate complete text from new metadata fields
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

    ## Generate timestamps from new metadata fields
    generate_timestamps_q = """ UPDATE t2_subreddit_metadata SET created_utc_ts = to_timestamp(created_utc) WHERE created_utc_ts IS NULL AND has_metadata = 1 """

    execute_in_db(generate_timestamps_q)


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


## Update moderator metadata table and processing queue
def pull_moderator_roles_if_not_in_db(subreddit, moderators):
    check_for_mod_metadata_sql = """ SELECT has_metadata FROM t2_moderator_metadata WHERE username = '{}' """
    add_moderator_roles_to_table_sql = """ INSERT INTO t2_moderator_metadata (username, subreddits_moderated, has_metadata) VALUES (%s, %s, 1) """
    mark_moderator_unsuccessful_sql = """ INSERT INTO t2_moderator_metadata (username, has_metadata) VALUES (%s, -1) """

    request_count = 0
    
    for moderator in moderators:
#        moderator = moderators.pop()
        moderator_metadata_in_db = execute_in_db(check_for_mod_metadata_sql.format(moderator), return_first_only = True)
        if not moderator_metadata_in_db:
            moderated_subreddits = scrape_moderator_roles(reddit, moderator)
            if moderated_subreddits:
                assert len(moderated_subreddits) == len(set(moderated_subreddits))
            if moderated_subreddits:
#                print("MODERATED SUBREDDITS: {}\n\n".format(", ".join(moderated_subreddits)), flush=True)
                execute_in_db(add_moderator_roles_to_table_sql, args = [moderator, moderated_subreddits])
            else:
                print("\tmarking {} unsuccessful".format(moderator))
                execute_in_db(mark_moderator_unsuccessful_sql, args = [moderator])

        request_count += 1
        ## Sleep for a bit so we don't overload the API
        if (request_count % 60 == 0):
            print("\tCurrently collecting {}.... Request count #{}.... Sleeping 60 seconds.".format(moderator, request_count), flush=True)
            sleep(60)


######################
## Running snowball ##
######################

def check_num_unprocessed(queue_table):
    unprocessed_count_q = """ SELECT COUNT(*) FROM {} WHERE processed = 0 """.format(queue_table)
    unprocessed_count = execute_in_db(unprocessed_count_q, return_first_only = True)[0]
    return unprocessed_count


def shared_moderator_snowball():
    get_unprocessed_sql = """ SELECT subreddit, step FROM t1c_moderator_queue WHERE processed = 0 ORDER BY step DESC, subreddit DESC"""
    check_for_sub_metadata_sql = """ SELECT has_metadata, has_moderator_metadata FROM t2_subreddit_metadata WHERE subreddit = '{}' """
    get_subreddit_moderators_sql = """ SELECT TRIM(JSON_ARRAY_ELEMENTS(moderators)::TEXT, '"') FROM
        t2_subreddit_metadata WHERE subreddit = '{}' """
    set_sub_moderator_metadata_sql = """ UPDATE t2_subreddit_metadata SET
        has_moderator_metadata = 1 WHERE subreddit = '{}' """
    get_subreddits_in_processing_queue_sql = """ SELECT subreddit FROM
        t1c_moderator_queue """
    add_to_processing_queue_sql = """ INSERT INTO t1c_moderator_queue
        (subreddit, processed, step) VALUES (%s, 0, %s) """
    add_ties_sql = """ INSERT INTO t1c_moderator_ties (source, target, label)
        VALUES (%s, %s, %s) """
    set_processed_sql = """ UPDATE t1c_moderator_queue SET processed = 1 WHERE
        subreddit = '{}' """
    set_processing_unsuccessful_sql = """ UPDATE t1c_moderator_queue SET
        processed = -1 WHERE subreddit = '{}' """
    add_to_metadata_table_sql = """ INSERT INTO t2_subreddit_metadata (subreddit, has_metadata) VALUES (%s, 0) """
    get_moderation_roles_sql = """ SELECT UNNEST(subreddits_moderated) FROM t2_moderator_metadata WHERE username = '{}' AND skip = 0 """
    get_subreddits_in_metadata_table_sql = """ SELECT subreddit FROM t2_subreddit_metadata """
    
    queue = execute_in_db(get_unprocessed_sql, return_results = True)

    while queue:
        subreddit, step = queue.pop()
        print("subreddit={}, step={}".format(subreddit, step))
        ## Scrape the subreddit metadata as needed
        metadata_in_db, all_moderator_metadata_in_db = execute_in_db(check_for_sub_metadata_sql.format(subreddit), return_results = True)[0]
        if metadata_in_db == 0:
            ## Scrape the metadata
            metadata_row = scrape_subreddit_metadata(reddit, subreddit)
            ## Add it to the database
            insert_subreddit_metadata_row(metadata_row, subreddit)
        elif metadata_in_db == -1:
            ## We can't pull the metadata, so we won't be able to get the shared moderator ties
            execute_in_db(set_processing_unsuccessful_sql.format(subreddit))
            continue
        
		## Scrape other moderation roles for the subreddit's moderators as needed
        moderators = execute_in_db(get_subreddit_moderators_sql.format(subreddit), return_first_only = True)
        if all_moderator_metadata_in_db == 0:
            ## Scrape the moderation roles and add to database as needed
            pull_moderator_roles_if_not_in_db(subreddit, moderators)
            ## Mark that we have the moderation roles
            execute_in_db(set_sub_moderator_metadata_sql.format(subreddit))

        ## Now that we have all the metadata we need, reconstruct shared moderator ties
        ## Start a list of shared moderator ties
        shared_moderator_ties = []
        ## Keep track of subreddits discovered at this step
        moderated_subreddits = set()
        ## For each of the subreddit's moderators:
        for moderator in moderators:
            ## Pull the other subreddits moderated by this account
            moderation_roles = execute_in_db(get_moderation_roles_sql.format(moderator), return_first_only = True)
            ## For each of the other moderated subreddits:
            for moderated_subreddit in moderation_roles:
                ## Add the shared moderator edge to the list of ties
                if moderated_subreddit != subreddit:
                    assert len(shared_moderator_ties) == len(set(shared_moderator_ties))
                    shared_moderator_ties.append((subreddit, moderated_subreddit, moderator))
                    ## Keep track of the moderated subreddits, so we can process them later if we need to
                    moderated_subreddits.add(moderated_subreddit)
        
        ## Get a list of all the subreddits in the processing queue
        subreddits_in_queue = set(execute_in_db(get_subreddits_in_processing_queue_sql, return_first_only = True))
        ## Get a list of all the subreddits in the metadata table
        subreddits_in_metadata_table = set(execute_in_db(get_subreddits_in_metadata_table_sql, return_first_only = True))
        ## See if any of the moderated subreddits aren't in the processing queue
        subreddits_to_add_to_queue = moderated_subreddits.difference(subreddits_in_queue)
        ## See if any of the moderated subreddits aren't in the metadata table
        subreddits_to_add_to_metadata_table = moderated_subreddits.difference(subreddits_in_metadata_table)
        ## Add them to the processing queue and metadata table
        if subreddits_to_add_to_metadata_table:
            execute_in_db(add_to_metadata_table_sql, args = [(sub.lower(),) for sub in subreddits_to_add_to_metadata_table], batch_insert = True)
        if subreddits_to_add_to_queue:
            execute_in_db(add_to_processing_queue_sql, args = [(sub, step + 1) for sub in subreddits_to_add_to_queue], batch_insert = True)

        ## Add the new ties to the table
        if shared_moderator_ties:
            execute_in_db(add_ties_sql, args = shared_moderator_ties, batch_insert = True)

        ## Mark the subreddit as processed in the queue
        execute_in_db(set_processed_sql.format(subreddit))


## Get an authenticated Reddit API object
reddit = init_reddit()

# Scrape shared moderator edges
shared_moderator_snowball()

print("done")
