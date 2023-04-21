import json
import requests
from time import sleep, strftime
import psycopg
import pytz

'''
Output: a json file with the following structure:
- "keywords": a list of the search keywords used
- "timestamp": the current timestamp when the results were dumped into the json file
- "results": a list of search results, where each row has the following fields in order:
    (1) search keyword,
    (2) subreddit name (i.e. r/something),
    (3) subreddit id (i.e. a string starting with t_),
    (4) subreddit title,
    (5) subreddit description as shown in the search results page,
    (6) a link to the subreddit, and
    (7) the concatenation of the description and title (for easier text searching/matching).
    Note: Some fields may be empty if the subreddit is private or otherwise not publicly visible.
'''

db_config_path = "/Users/lgs17/Desktop/Reddit Collab 2022/code/db_config.txt"
database_table = "t0_keyword_search"

def pull_keywords():
    keywords = ["red pill", "redpill", "trp", "blue pill", "bluepill", "manosphere", "mra", "men's rights movement", "men's rights activists", "mgtow", "mghow", "men going their own way", "mgtower", "pua", "pickup artist", "pick-up artist", "feminism", "feminist", "misandry", "genderqueer", "trans", "ftm", "mtf", "transgender", "transsexual", "non-binary", "enby", "nonbinary", "chad", "stacy", "becky", "foid", "femoid", "alpha", "beta", "zeta", "incel"]
    return keywords

def extract_subreddits_from_json(result, keyword):

    extracted_subreddits = []
    subreddits = result["data"]["children"]

    for sub_json in subreddits:

        subreddit = sub_json["data"]["display_name"]
        subreddit_title = sub_json["data"]["title"]
        description = sub_json["data"]["description"]
        link = "https://www.reddit.com/{}".format(sub_json["data"]["url"])
        subreddit_id = sub_json["data"]["name"]
        searched_text = " ".join([elt for elt in [subreddit_title, description] if elt])

        row = (keyword, subreddit.lower(), subreddit_id, subreddit_title, description, link, searched_text)
        extracted_subreddits.append(row)

    return extracted_subreddits


def pull_keyword_search_results(keyword):

    keyword_rows = []

    def make_request(after_id=None):
        
        # print("\n\t\tInside make_request(after_id={})".format(after_id))
        
        search_endpoint = "https://www.reddit.com/subreddits/search.json"
        search_headers = {"User-agent": "com.lgs17.searching_subreddits"}

        search_params = {
            "q": keyword,
            "include_over_18": "on",
            "limit": 100,
            "show": "all",
            "raw_json": 1
        }

        if after_id:
            search_params["after"] = after_id

        result = requests.get(search_endpoint, headers = search_headers, params = search_params)
        # print("\t\t{}".format(result.url), flush=True)

        if result.ok:
            page_results = extract_subreddits_from_json(result.json(), keyword)
            last_id = page_results[-1][2] if bool(page_results) else None
            # print("\t\t{} results".format(len(page_results)), flush=True)
            if len(page_results) == 0:
                return None

            if last_id != after_id:
                keyword_rows.extend(page_results)
            
            # print("\t\tFirst result: {}".format(page_results[0][1]), flush=True)
            # print("\t\tLast result: {}\n".format(page_results[-1][1]), flush=True)

            return last_id

        else:
            print("\tBad response: {}".format(str(result)), flush=True)
            return


    new_after_id, after_id = make_request(), ""
    request_count = 0

    while bool(new_after_id):
        # print("\n\tWhile Loop:", flush=True)
        after_id = new_after_id
        new_after_id = make_request(after_id=new_after_id)

        request_count = request_count + 1
        # print("\t\tRequests for {}: {}".format(keyword, str(request_count)), flush=True)

        if (request_count % 60 == 0) or not bool(new_after_id):
            print("\t\tCurrently collecting {}.... Request count #{}.... {} rows collected.... Sleeping 60 seconds.".format(keyword, request_count, len(keyword_rows)), flush=True)
            # print("\t\tLast subreddit: {}".format(keyword_rows[-1][1]), flush=True)
            sleep(60)

        ## There are not enough results for a new iteration, so we're getting duplicates of the first results
        if new_after_id == after_id:
            new_after_id = None

        # print("\t\tafter_id = {}, new_after_id = {}".format(after_id, new_after_id), flush=True)
    
    # print("\t{} rows collected.".format(len(keyword_rows)))
    return keyword_rows

## Get authenticated conn and cursor objects (returns conn, cursor tuple)
def get_db():
    with open(db_config_path) as f:
        conn_str = " ".join([l.strip() for l in f.readlines()])
    conn = psycopg.connect(conn_str)
    cursor = conn.cursor()
    return (conn, cursor)

def execute_in_db(query, return_results = False, return_first_only = False, args = None, batch_insert = False):
    conn, cursor = get_db()

    if args and batch_insert:
        for row in args:
            cursor.execute(query, row)
    if args:
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


keywords = pull_keywords()


all_rows = []
for index, keyword in enumerate(keywords):
    print("{} rows collected.... ({}/{} or {}%)".format(len(all_rows), (index+1), len(keywords), int((index+1)*100/len(keywords))), flush=True)
    print("CURRENT KEYWORD:\t{}".format(keyword))
    keyword_rows = pull_keyword_search_results(keyword)
    all_rows.extend(keyword_rows)


print("Removing duplicates....", flush=True)
all_rows = list(set(all_rows))

print("{} rows collected. Now dumping to file....".format(len(all_rows)), flush=True)
current_ts = strftime("%Y-%m-%d")
with open("outputs/seeds_subreddits_collected_{}.json".format(current_ts), "w+") as f:
    output = {
        "keywords": keywords,
        "timestamp": current_ts,
        "results": all_rows
    }
    json.dump(output, f)
    
print("Now adding to database...")
for row in all_rows:
    print(row[0:3], flush=True)
    print("\n", flush=True)
    insert_q = """ INSERT INTO t0_keyword_search (keyword, subreddit, subreddit_id, subreddit_title, description, link, searched_text) VALUES (%s,%s,%s,%s,%s,%s,%s) """
    
    execute_in_db(query = insert_q, args = row)

execute_in_db(query = """ INSERT INTO t2_subreddit_metadata (subreddit, has_metadata, seed) SELECT DISTINCT subreddit, 0, 1 FROM t0_keyword_search """)    

print("Done!")
