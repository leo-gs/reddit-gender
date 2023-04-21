from time import sleep, strftime
import pandas as pd
import praw
import prawcore
import json
import os
import sys


seed_subreddit_path = sys.argv[1]
if not os.path.exists(seed_subreddit_path):
    print("Seed file not found: ".format(seed_subreddit_path), flush=True)
    quit()

reddit_config_path = "/Users/lgs17/Desktop/Reddit Collab 2022/code/reddit_config.txt"
if not os.path.exists(reddit_config_path):
    print("Reddit creds file not found: ".format(reddit_config_path), flush=True)
    quit()

subreddit_metadata_dir = "outputs/subreddit_metadata/"
if not os.path.exists(subreddit_metadata_dir):
    print("Metadata output file not found: ".format(subreddit_metadata_dir), flush=True)
subreddit_metadata_fpath = subreddit_metadata_dir + "{}.json"

current_ts = strftime("%Y-%m-%d")

def init_reddit():
    print("Creating Reddit object...", flush=True)
    reddit_config = {}
    with open(reddit_config_path) as f:
        for line in f.readlines():
            key, value = line.split("=")
            reddit_config[key.strip()] = value.strip()
        reddit = praw.Reddit(**reddit_config)
        return reddit

def pull_seed_subreddit_names_from_file():
    print("Reading seed subreddit file...", flush=True)
    with open(seed_subreddit_path) as f:
        seed_data = json.load(f)
        seed_subs = list(set([row[1] for row in seed_data["results"]]))
        return sorted(seed_subs)

def pull_already_collected_subreddit_names():
    return [fpath.replace(".json", "") for fpath in os.listdir(subreddit_metadata_dir)]

def scrape_subreddit(reddit, subreddit_name, index):
    if index % 100 == 0:
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

def write_subreddit_metadata(subreddit, row):
    with open(subreddit_metadata_fpath.format(subreddit), "w") as f:
        output = {
            "metadata": row,
            "collected_date": current_ts
        }
        json.dump(output, f)

def scrape_subreddit_metadata(subreddit_names):
    reddit = init_reddit()
    for index, sub in enumerate(subreddit_names):
        row = scrape_subreddit(reddit, sub, index)
        write_subreddit_metadata(sub, row)

subreddits_to_collect = pull_seed_subreddit_names_from_file()
print("{} seed subreddits pulled from file.".format(len(subreddits_to_collect)), flush=True)

already_collected = pull_already_collected_subreddit_names()
print("{} seed subreddits already have metadata.".format(len(already_collected)), flush=True)

subreddits_to_collect = list(set(subreddits_to_collect) - set(already_collected))
print("{} seed subreddits remaining.".format(len(subreddits_to_collect)), flush=True)

print("Beginning metadata collection...")
scrape_subreddit_metadata(subreddits_to_collect)

print("Done!")

