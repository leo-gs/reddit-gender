# Keyword-Snowball Subreddit Sampling for studying gender communities on Reddit

This code accompanies the paper Nobody Puts Redditor in a Binary: Digital Demography, Collective Identities, and Gender in a Subreddit Network (ACM page)[https://dl.acm.org/doi/10.1145/3449082].

**/database_scripts:**

These scripts are intended to be run in sequence.
* 0_create_tables.sql: prepares a given database for collecting subreddit metadata and network relations.
* 1_collect_seeds_by_keywords.py: searches Reddit's subreddit search for all ("seed") subreddits matching the keywords specified in the method pull_keywords().
* 2_hyperlink_tracing.py: reconstructs hyperlink/reference ties for all subreddits in the hyperlink processeing queue table.
* 3_shared_moderator_tracing.py: reconstructs shared moderator ties for all subreddits in the shared moderator processing queue table.
