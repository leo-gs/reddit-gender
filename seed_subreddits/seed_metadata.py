from time import sleep
import pandas as pd
import praw
import prawcore
import psycopg2
from psycopg2 import extras as ext
import json
import os


def init_reddit():
    print("Creating Reddit object...", flush=True)
    reddit_config = {}
    with open("~/Desktop/Reddit Collab 2022/code/reddit_config.txt") as f:
        for line in f.readlines():
            key, value = line.split("=")
            reddit_config[key.strip()] = value.strip()
        reddit = praw.Reddit(**reddit_config)
        return reddit


#def pull_subreddits():
#    SELECT_STMT = """
#    SELECT subreddit
#    FROM keyword_subreddit_search
#    """
#
#    conn_string = "host='techne.ischool.uw.edu' dbname='transmasc_reddit_2' user='lgs17' password='WinCoo11!!'"
#    conn = psycopg2.connect(conn_string)
#    cursor = conn.cursor()
#
#    cursor.execute(SELECT_STMT)
#
#    subreddits = [row[0] for row in cursor.fetchall()]
#    print(len(subreddits))
#
#    conn.commit()
#    cursor.close()
#    conn.close()
#
#    return subreddits


def scrape_subreddit(reddit, subreddit_name, index):
    print("Scraping #{}: {}...".format(index, subreddit_name), flush=True)

    ## Retrieve a PRAW subreddit object for the given subreddit name
    def get_subreddit(reddit, subreddit_name):
        return reddit.subreddit(subreddit_name)

    ## Pull JSON data a post's author and cache it
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
    except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden, AttributeError) as ex:
        return
    
    moderators = json.dumps([pull_author(moderator) for moderator in subreddit.moderator()])

    row = [subreddit_name] + [str(subreddit.__dict__.get(key)) for key in ["display_name", "free_form_reports", \
    "subreddit_type", "community_icon", "banner_background_image", "header_title", "over18", \
    "show_media", "description", "title", "collapse_deleted_comments", "id", "emojis_enabled", \
    "can_assign_user_flair", "allow_videos", "spoilers_enabled", "active_user_count", \
    "original_content_tag_enabled", "display_name_prefixed", "can_assign_link_flair", \
    "submit_text", "allow_videogifs", "accounts_active", "public_traffic", "subscribers", \
    "all_original_content", "lang", "has_menu_widget", "name", "user_flair_enabled_in_sr", \
    "created", "url", "quarantine", "hide_ads", "created_utc", "allow_discovery", "accounts_active_is_fuzzed", \
    "advertiser_category", "public_description", "link_flair_enabled", "allow_images", "videostream_links_count", \
    "comment_score_hide_mins", "show_media_preview", "submission_type"]] + [moderators]
    return row


def load_metadata():
    if not os.path.isfile("subreddit_metadata_backup.json"):
        reddit = init_reddit()
        subreddits = pull_subreddits()
        subreddit_metadata_rows = [scrape_subreddit(reddit, sub, index) for index, sub in enumerate(subreddits)]

        with open("subreddit_metadata_backup.json", "w+") as f:
            json.dump(subreddit_metadata_rows, f)

    else:
        with open("subreddit_metadata_backup.json") as f:
            subreddit_metadata_rows = json.load(f)

    subreddit_metadata_rows = [s for s in subreddit_metadata_rows if s]
    return subreddit_metadata_rows


def clean_rows(rows):
    for i, row in enumerate(rows):

        for j in range(len(rows[i])):
            if rows[i][j] == "None":
                rows[i][j] = None
    return rows



subreddit_metadata_rows = load_metadata()
subreddit_metadata_rows = clean_rows(subreddit_metadata_rows)

DROP_STMT = """
    DROP TABLE IF EXISTS subreddit_metadata_2;
"""

CREATE_STMT = """
    CREATE TABLE subreddit_metadata_2 (
        subreddit TEXT,
        display_name TEXT,
        free_form_reports BOOLEAN,
        subreddit_type TEXT,
        community_icon TEXT,
        banner_background_image TEXT,
        header_title TEXT,
        over18 BOOLEAN,
        show_media BOOLEAN,
        description TEXT,
        title TEXT,
        collapse_deleted_comments BOOLEAN,
        subreddit_id TEXT,
        emojis_enabled BOOLEAN,
        can_assign_user_flair BOOLEAN,
        allow_videos BOOLEAN,
        spoilers_enabled BOOLEAN,
        active_user_count INT,
        original_content_tag_enabled BOOLEAN,
        display_name_prefixed TEXT,
        can_assign_link_flair BOOLEAN,
        submit_text TEXT,
        allow_videogifs BOOLEAN,
        accounts_active INT,
        public_traffic BOOLEAN,
        subscribers INT,
        all_original_content BOOLEAN,
        lang TEXT,
        has_menu_widget BOOLEAN,
        name TEXT,
        user_flair_enabled_in_sr BOOLEAN,
        created FLOAT,
        url TEXT,
        quarantine BOOLEAN,
        hide_ads BOOLEAN,
        created_utc FLOAT,
        allow_discovery BOOLEAN,
        accounts_active_is_fuzzed BOOLEAN,
        advertiser_category TEXT,
        public_description TEXT,
        link_flair_enabled BOOLEAN,
        allow_images BOOLEAN,
        videostream_links_count INT,
        comment_score_hide_mins INT,
        show_media_preview BOOLEAN,
        submission_type TEXT,
        moderators JSON,
        PRIMARY KEY(subreddit)
    );
"""

INSERT_STMT = """
INSERT INTO subreddit_metadata_2 (subreddit, display_name, free_form_reports, subreddit_type, community_icon, 
    banner_background_image, header_title, over18, show_media, description, title, collapse_deleted_comments, 
    subreddit_id, emojis_enabled, can_assign_user_flair, allow_videos, spoilers_enabled, active_user_count, 
    original_content_tag_enabled, display_name_prefixed, can_assign_link_flair, submit_text, allow_videogifs, 
    accounts_active, public_traffic, subscribers, all_original_content, lang, has_menu_widget, name, user_flair_enabled_in_sr, 
    created, url, quarantine, hide_ads, created_utc, allow_discovery, accounts_active_is_fuzzed, advertiser_category, 
    public_description, link_flair_enabled, allow_images, videostream_links_count, comment_score_hide_mins, show_media_preview, 
    submission_type, moderators) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

#conn_string = "host='techne.ischool.uw.edu' dbname='transmasc_reddit_2' user='lgs17' password='WinCoo11!!'"
#conn = psycopg2.connect(conn_string)
#cursor = conn.cursor()

#cursor.execute(DROP_STMT)
#cursor.execute(CREATE_STMT)

#print("uploading {} rows....".format(len(subreddit_metadata_rows)), flush=True)
#ext.execute_batch(cursor, INSERT_STMT, subreddit_metadata_rows)
#print("finished uploading....", flush=True)

#conn.commit()
#cursor.close()
#conn.close()

#print("Done!")




