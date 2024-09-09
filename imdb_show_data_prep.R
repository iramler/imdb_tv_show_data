#The goal of this document is to outline a
# reproducible way to reduce the size of
# the IMDB datasets (TV Shows only) for the 
# projects and webapps SLU uses associated 
# with IMDb TV shows.


library(tidyverse)

#Start by importing the TV Show data from IMDb

# get list of popular show IDs
basics = read_tsv("https://datasets.imdbws.com/title.basics.tsv.gz")
ratings = read_tsv("https://datasets.imdbws.com/title.ratings.tsv.gz")

#Determine TV series

# Set parameters
current_year <- as.numeric(format(Sys.Date(), "%Y"))
start_year <- 1947  # Always take 2 shows from 1947 onward
top_n_start <- 100  # Start by selecting 100 shows for the current year

# Filter for TV series starting from 1947
tvseries <- basics %>%
  filter(titleType == "tvSeries") %>%
  filter(parse_number(startYear) >= start_year & parse_number(startYear) <= current_year)  # Filter between 1947 and current year

# Join with ratings and filter adult content
tvseries2 <- left_join(tvseries, ratings, by = "tconst") %>%
  filter(!isAdult) %>%
  select('tconst', 'primaryTitle','startYear','endYear','averageRating','numVotes')

# Create a ranking that selects the top N shows per year
top_tv <- tvseries2 %>%
  drop_na(averageRating, numVotes) %>%
  mutate(
    # Reduce by 2 each year starting from top_n_start, but ensure a minimum of 2 shows from 1947 onwards
    shows_to_select = ifelse(parse_number(startYear) >= start_year, 
                             pmax(top_n_start - 2 * (current_year - parse_number(startYear)), 2), 
                             0)  # Minimum of 2 shows from 1947 onwards
  ) %>%
  group_by(startYear) %>%
  mutate(
    Rating_Rank = min_rank(desc(averageRating)),
    nVotes_Rank = min_rank(desc(numVotes))
  ) %>%
  filter(Rating_Rank <= shows_to_select | nVotes_Rank <= shows_to_select)  # Select top shows for each year


# 
# 
# year = 1980
# top_n = 50
# 
# tvseries <- basics %>%
#   filter(titleType == "tvSeries") %>%
#   filter(parse_number(startYear) >= year)
# 
# tvseries2 <- left_join(tvseries, ratings, by = "tconst") %>%
#   filter(!isAdult) %>%
#   select('tconst', 'primaryTitle','startYear','endYear','averageRating','numVotes')
# 
# top_tv <-
#   tvseries2 %>%
#   drop_na(averageRating, numVotes) %>%
#   group_by(startYear) %>%
#   mutate(
#     Rating_Rank = min_rank(desc(averageRating)),
#     nVotes_Rank = min_rank(desc(numVotes))
#   ) %>%
#   filter(Rating_Rank <= top_n | nVotes_Rank <= top_n) 

#Load all episode data

episode = read_tsv("https://datasets.imdbws.com/title.episode.tsv.gz")


#1)  Subset Episodes (to keep only popular since `year`)
#2)  Attach Episode Ratings (and Votes)
#3)  Attach Episode Name


episode_popular <- episode %>%
  filter(parentTconst %in% top_tv$tconst) %>%
  left_join(ratings, by = "tconst") %>%
  left_join(basics %>% select(1,3), by = "tconst") %>%
  left_join(basics %>% select(1,3), by = c("parentTconst" = "tconst")) %>%
  rename(
    Episode_Name = primaryTitle.x,
    Show_Name = primaryTitle.y
  ) %>%
  left_join(top_tv %>% select("tconst","startYear"), 
            by = c("parentTconst" = "tconst"))

popular_tv_shows <- 
  episode_popular %>%
  ungroup() %>%
  select(
    "Show_Name", "startYear",
    "Episode_Name", 
    "seasonNumber", "episodeNumber", 
    "averageRating", "numVotes", "tconst", "parentTconst"
  ) %>%
  mutate(startYear = parse_number(startYear))

# dump commas from show and episode names...
# also dump NAs in the ratings and/or Votes

popular_tv_shows <- popular_tv_shows %>%
  mutate(Show_Name = str_remove_all(Show_Name,pattern = ","),
         Episode_Name = str_remove_all(Episode_Name,pattern = ","),
         seasonNumber = parse_number(seasonNumber),
         episodeNumber_in_season = parse_number(episodeNumber)
  ) %>%
  drop_na(averageRating, seasonNumber, episodeNumber_in_season) %>%
  filter(episodeNumber_in_season > 0.01) %>%
  group_by(Show_Name) %>%
  arrange(seasonNumber, episodeNumber_in_season) %>%
  mutate(episodeNumber = row_number()) %>%
  relocate(episodeNumber_in_season, .after = seasonNumber) %>%
  rename(episodeNumber_overall = episodeNumber)


# Save the popular tv show file info

# show names (to build the list)
# Create the directory to store the files if it doesn't exist
if (!dir.exists("data")) {
  dir.create("data")
}


popular_tv_shows %>%
  group_by(Show_Name) %>%
  summarize(startYear = min(startYear),
            xbarNV = mean(numVotes, na.rm = TRUE),
            parentTconst = first(parentTconst)
  ) %>%
  ungroup() %>%
  arrange(desc(xbarNV)) %>% select(Show_Name, parentTconst) -> show_names

saveRDS(object = show_names, file = "data/show_names.rds")


# individual show files
# Assuming `popular_tv_shows` is your data frame containing all the episode data


# Split the data by the 'parentTconst' (series ID) and write each to a CSV file
popular_tv_shows %>%
  group_by(parentTconst) %>%   # Group the data by the 'parentTconst' column
  group_split() %>%            # Split the grouped data into a list of data frames
  walk(~ write.csv(.x, 
                   file = paste0("data/", unique(.x$parentTconst), ".csv"),
                   row.names = FALSE))


