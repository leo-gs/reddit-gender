/*
Note: this setup requires any subreddits in the queue or tie tables to be in the metadata table as well
For this to work with the hyperlink tracing script, you'll need to insert some seed subreddits to t2_subreddit_metadata, e.g.

INSERT INTO t2_subreddit_metadata (subreddit, has_metadata, seed)
VALUES ('dogs', 0, 1), ('cats', 0, 1), ...

That script will first scrape the metadata for the seed subreddits, then start the hyperlink snowballs process.
*/

CREATE TABLE t2_subreddit_metadata (
	subreddit text NOT NULL,
	display_name text NULL,
	free_form_reports bool NULL,
	subreddit_type text NULL,
	community_icon text NULL,
	banner_background_image text NULL,
	header_title text NULL,
	over18 bool NULL,
	show_media bool NULL,
	description text NULL,
	title text NULL,
	collapse_deleted_comments bool NULL,
	subreddit_id text NULL,
	emojis_enabled bool NULL,
	can_assign_user_flair bool NULL,
	allow_videos bool NULL,
	spoilers_enabled bool NULL,
	active_user_count int4 NULL,
	original_content_tag_enabled bool NULL,
	display_name_prefixed text NULL,
	can_assign_link_flair bool NULL,
	submit_text text NULL,
	allow_videogifs bool NULL,
	accounts_active int4 NULL,
	public_traffic bool NULL,
	subscribers int4 NULL,
	all_original_content bool NULL,
	lang text NULL,
	has_menu_widget bool NULL,
	name text NULL,
	user_flair_enabled_in_sr bool NULL,
	created float8 NULL,
	url text NULL,
	quarantine bool NULL,
	hide_ads bool NULL,
	created_utc float8 NULL,
	allow_discovery bool NULL,
	accounts_active_is_fuzzed bool NULL,
	advertiser_category text NULL,
	public_description text NULL,
	link_flair_enabled bool NULL,
	allow_images bool NULL,
	videostream_links_count int4 NULL,
	comment_score_hide_mins int4 NULL,
	show_media_preview bool NULL,
	submission_type text NULL,
	moderators json NULL,
	rules json NULL,
	created_utc_ts timestamp NULL,
	has_metadata int2 NULL DEFAULT 0,
	complete_metadata_text text NULL,
	seed int2 NULL DEFAULT 0,
	has_moderator_metadata int2 NULL DEFAULT 0,
    retrieved_date timestamp DEFAULT NOW(),
	CONSTRAINT t2_subreddit_metadata_pkey PRIMARY KEY (subreddit)
);


CREATE TABLE t1a_hyperlink_queue (
	subreddit text NOT NULL,
	processed int2 NULL DEFAULT 0,
	step int2 NULL,
    added_to_queue_date timestamp DEFAULT NOW(),
	CONSTRAINT t1a_hyperlink_queue_pkey PRIMARY KEY (subreddit),
	CONSTRAINT t1a_hyperlink_queue_subreddit_fkey FOREIGN KEY (subreddit) REFERENCES t2_subreddit_metadata(subreddit)
);

CREATE TABLE public.t1a_hyperlink_ties (
	source text NOT NULL,
	target text NOT NULL,
	label text NULL,
	CONSTRAINT t1a_hyperlink_ties_pkey PRIMARY KEY (source, target),
	CONSTRAINT t1a_hyperlink_ties_source_fkey FOREIGN KEY (source) REFERENCES t2_subreddit_metadata(subreddit),
	CONSTRAINT t1a_hyperlink_ties_target_fkey FOREIGN KEY (target) REFERENCES t2_subreddit_metadata(subreddit)
);

CREATE TABLE t1b_reference_queue (
	subreddit text NOT NULL,
	processed int2 NULL DEFAULT 0,
	step int2 NULL,
    added_to_queue_date timestamp DEFAULT NOW(),
	CONSTRAINT t1b_reference_queue_pkey PRIMARY KEY (subreddit),
	CONSTRAINT t1b_reference_queue_subreddit_fkey FOREIGN KEY (subreddit) REFERENCES t2_subreddit_metadata(subreddit)
);

CREATE TABLE public.t1b_reference_ties (
	source text NOT NULL,
	target text NOT NULL,
	label text NULL,
	CONSTRAINT t1b_reference_ties_pkey PRIMARY KEY (source, target),
	CONSTRAINT t1b_reference_ties_source_fkey FOREIGN KEY (source) REFERENCES t2_subreddit_metadata(subreddit),
	CONSTRAINT t1b_reference_ties_target_fkey FOREIGN KEY (target) REFERENCES t2_subreddit_metadata(subreddit)
);

CREATE TABLE t0_keyword_search (
    keyword text NOT NULL,
    subreddit text NOT NULL,
    subreddit_id text NOT NULL,
    subreddit_title text,
    description text,
    link text,
    searched_text text,
    retrieved_date timestamp DEFAULT NOW(),
    PRIMARY KEY (keyword, subreddit)
);

CREATE TABLE t1c_moderator_queue (
    subreddit text NOT NULL,
	processed int2 NULL DEFAULT 0,
	step int2 NULL,
    added_to_queue_date timestamp DEFAULT NOW(),
    CONSTRAINT t1c_moderator_queue_pkey PRIMARY KEY (subreddit),
    CONSTRAINT t1c_moderator_queue_subreddit_fkey FOREIGN KEY (subreddit) REFERENCES t2_subreddit_metadata(subreddit)
);

CREATE TABLE t2_moderator_metadata(
    username text NOT NULL,
    subreddits_moderated text[] NULL,
    has_metadata int2 DEFAULT 0,
    skip int2 DEFAULT 0,
    retrieved_date timestamp DEFAULT NOW(),
    CONSTRAINT t2_moderator_metadata_pkey PRIMARY KEY (username)
);

CREATE TABLE public.t1c_moderator_ties (
    source text NOT NULL,
    target text NOT NULL,
    label text NOT NULL,
    CONSTRAINT t1c_moderator_ties_pkey PRIMARY KEY (source, target, label),
    CONSTRAINT t1c_moderator_ties_source_fkey FOREIGN KEY (source) REFERENCES t2_subreddit_metadata(subreddit),
    CONSTRAINT t1c_moderator_ties_target_fkey FOREIGN KEY (target) REFERENCES t2_subreddit_metadata(subreddit),
    CONSTRAINT t1c_moderator_ties_label_fkey FOREIGN KEY (label) REFERENCES t2_moderator_metadata(username)
);
