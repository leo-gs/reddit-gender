rm(list = ls())
knitr::opts_chunk$set(echo = FALSE, message = FALSE)
library(tidyverse)
library(jsonlite)
library(purrr)
library(igraph)
library(networkD3)

seeds_fname <- "seeds_subreddits_collected_2023-01-11.json"
seeds_js <- read_json(path = paste0("~/Desktop/Reddit Collab 2022/code/reddit-gender/seed_subreddits/outputs/", seeds_fname, collapse = ""))

keywords <- unlist(seeds_js$keywords)
timestamp <- seeds_js$timestamp

results <- lapply(seeds_js$results, function(seedrow) {
  tibble(
    keyword = seedrow[[1]],
    subreddit = seedrow[[2]],
    id = seedrow[[3]],
    title = seedrow[[4]],
    description = if_else(is.null(seedrow[[5]]), NA_character_, seedrow[[5]]),
    link = seedrow[[6]],
    combined_text_fields = seedrow[[7]]
  )
}) %>% bind_rows()

new_kws <- c("chad", "stacy", "becky", "foid", "femoid", "alpha", "beta", "zeta", "incel")

results <- results %>%
  mutate(orig_kw_result = as.integer(!(keyword %in% new_kws))) %>%
  mutate(has_text_match = as.integer(str_detect(combined_text_fields, keyword))) %>%
  mutate(orig_kw_AND_has_text_match = sign(orig_kw_result & has_text_match))

subreddits <- unique(results$subreddit)

collapsed_by_kw <- results %>%
  group_by(keyword) %>%
  summarise(
    keyword, subreddits = str_c(subreddit, collapse = ", "), .groups = "keep"
  ) %>%
  as_tibble() %>%
  unique() %>%
  mutate(subreddits = str_split(subreddits, pattern = ", "))

collapsed_by_subreddit <- results %>%
  group_by(subreddit) %>%
  summarise(
    subreddit, keywords = str_c(keyword, collapse = ", "), .groups = "keep"
  ) %>%
  as_tibble() %>%
  unique() %>%
  mutate(keywords = str_split(keywords, pattern = ", "))

jaccard_idx <- function(set1, set2) length(intersect(set1, set2)) / length(union(set1, set2))

directional_jidx <- function(set1, set2) length(intersect(set1, set2)) * 100 / length(set1)


M1 <- matrix(
  nrow = length(keywords),
  ncol = length(keywords),
  dimnames = list(keywords, keywords)
)

for (i in 1:length(keywords)) {
  kw1 <- keywords[i]
  
  for (j in 1:length(keywords)) {
    kw2 <- keywords[j]
    
    M1[kw1, kw2] <- directional_jidx(
      collapsed_by_kw %>%
        filter(keyword == kw1) %>%
        pull(subreddits) %>%
        unlist(),
      collapsed_by_kw %>%
        filter(keyword == kw2) %>%
        pull(subreddits) %>%
        unlist()
    )
  }
}


M2 <- matrix(
  nrow = length(subreddits),
  ncol = length(subreddits),
  dimnames = list(subreddits, subreddits)
)

for (i in 1:length(subreddits)) {
  sub1 <- subreddits[i]
  
  for (j in 1:length(subreddits)) {
    sub2 <- subreddits[j]
    
    M2[sub1, sub2] <- directional_jidx(
      collapsed_by_subreddit %>%
        filter(subreddit == sub1) %>%
        pull(keywords) %>%
        unlist(),
      collapsed_by_subreddit %>%
        filter(subreddit == sub2) %>%
        pull(keywords) %>%
        unlist()
    )
    
    
  }
}

```

```{r}
png(
  filename = "~/Desktop/Reddit Collab 2022/code/reddit-gender/seed_subreddits/outputs/seed_overlap.png",
  width = 800,
  height = 800,
  units = "px"
)

corrplot::corrplot(
  title = paste0("Percentage Overlap, ", seeds_fname, collapse = ""),
  M,
  method = "shade",
  is.corr = FALSE,
  diag = FALSE,
  tl.cex = 1,
  tl.col = "black",
  number.cex = 0.5,
  cl.cex = 1,
  addgrid.col = "grey",
  type = "full"
)

dev.off()