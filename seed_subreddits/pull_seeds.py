import json
import requests
from time import sleep, strftime



def pull_keywords():
    keywords = ["red pill", "redpill", "trp", "blue pill", "bluepill", "manosphere", "mra", "men's rights movement", "men's rights activists", "mgtow", "mghow", "men going their own way", "mgtower", "pua", "pickup artist", "pick-up artist", "feminism", "feminist", "misandry", "genderqueer", "trans", "ftm", "mtf", "transgender", "transsexual", "non-binary", "enby", "nonbinary"]
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



def pull_results(keyword):

    keyword_rows = []

    def make_request(after_id=None):
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

        if result.ok:
            page_results = extract_subreddits_from_json(result.json(), keyword)
            last_id = page_results[-1][1] if bool(page_results) else None

            if len(page_results) == 0:
                return None

            if last_id != after_id:
                keyword_rows.extend(page_results)

            return last_id

        else:
            print("Bad response: {}".format(str(result)), flush=True)
            return


    new_after_id, after_id = make_request(), ""
    request_count = 0

    while bool(new_after_id):
        after_id = new_after_id
        new_after_id = make_request(after_id=new_after_id)

        request_count = request_count + 1

        if (request_count % 60 == 0) or not bool(new_after_id):
            print("\tCurrently collecting {}.... Request count #{}.... {} rows collected.... Sleeping 60 seconds.".format(keyword, request_count, len(keyword_rows)), flush=True)
            print("\t\tLast subreddit: {}".format(keyword_rows[-1][-1]), flush=True)
            sleep(60)

        ## There are not enough results for a new iteration, so we're getting duplicates of the first results
        if new_after_id == after_id:
            new_after_id = None

        print("\tafter_id = {}, new_after_id = {}".format(after_id, new_after_id), flush=True)

    return keyword_rows



keywords = pull_keywords()



all_rows = []
for index, keyword in enumerate(keywords):
    print("{} rows collected.... Now collecting subreddits for keyword {} ({}/{} or {}%)".format(len(all_rows), keyword, (index+1), len(keywords), int((index+1)*100/len(keywords))), flush=True)
    keyword_rows = pull_results(keyword)
    all_rows.extend(keyword_rows)


print("Removing duplicates....", flush=True)
all_rows = list(set(all_rows))


with open("seeds_subreddits_collected_{}.json".format(strftime("%Y-%m-%d")), "w+") as f:
    json.dump(all_rows, f)

print("Done!")
