CREATE DATABASE IF NOT EXISTS reddit_db;
USE reddit_db;

CREATE TABLE IF NOT EXISTS reddit_posts (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  platform_id VARCHAR(32) NOT NULL UNIQUE,
  subreddit VARCHAR(64),
  author_mask VARCHAR(64),
  title TEXT,
  selftext MEDIUMTEXT,
  created_utc DATETIME,
  url TEXT,
  is_ad BOOLEAN,
  keywords JSON,
  clean_text MEDIUMTEXT
);

CREATE INDEX idx_subreddit_created ON reddit_posts (subreddit, created_utc);
