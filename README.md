# 1. Data Collection & Database Setup

## 1.1 Run the database schema
Run the provided schema file to create the database and table.

```bash
mysql -u <YOUR_MYSQL_USER> -p < schema.sql

```

This creates the `reddit_db` database and the `reddit_posts` table.

---

## 1.2 Create a Reddit app (API credentials)
1. Go to: [Reddit Apps](https://www.reddit.com/prefs/apps)
2. Click **“create app”**.
3. Fill out:
   - **name:** `lab5-scraper`
   - **type:** `script`
   - **redirect uri:** `http://localhost:8080`
4. Copy down:
   - **client_id** (short string under the app name)
   - **client_secret** (labeled “secret”)
   - Create your own **user_agent** string (e.g., `lab5-scraper by u/<your_username>`)

---

## 1.3 Create the `.env` file
Create a file named `.env` in the same directory as your Python script.

```
# Reddit API
PRAW_CLIENT_ID=<your_client_id>
PRAW_CLIENT_SECRET=<your_client_secret>
PRAW_USER_AGENT=lab5-scraper by u/<your_username>

# MySQL
MYSQL_HOST=localhost
MYSQL_USER=<your_mysql_user>
MYSQL_PASSWORD=<your_mysql_password>
MYSQL_DB=reddit_db
MYSQL_PORT=3306
```

---

## 1.4 Run the scraper script
Use the following command:

```bash
python3 DSCI560_Lab5_Data_collection.py --subreddit technology --limit 500
```

The script will:
- Fetch posts with PRAW
- Preprocess text (clean, mask usernames)
- Upsert into MySQL (`ON DUPLICATE KEY UPDATE` ensures no duplicates)

---

# 2. Data Preprocessing & Feature Engineering

## 2.1 DB migration
Purpose: add columns to store preprocessing and feature results:
- `embedding` (JSON) — stores embedding vectors as JSON.
- `ocr_text` (TEXT) — OCR-extracted text from images (optional).
- `idx_created_utc` index — speeds up time-range queries.

Recommended steps (backup first):

1. Backup the DB:
```bash
# interactive
mysqldump -u root -p reddit_db > ~/reddit_db_backup_$(date +%F).sql
```
2. Run migration:
```bash
python3 run_migration.py
```

## 2.2 Preprocessing

Install the required library by running
```bash
pip install -r requirements.txt
```

Then you can run the preprocessing to create the embedded data by
```bash
python3 preprocessing.py
```

You can also see the result summary by running
```bash
python3 preprocessing.py --stats
```

# 3. Running the Pipeline
## 3.1 Data Collection + Processing + CLI

```bash
$ python main.py --subreddit tech --interval 5
```

- ```--subreddit``` tech → collect from r/tech
- --```interval 5``` → collect continuously for 5 minutes
- After collection, pipeline runs embeddings + clustering + visualization
- Then enters CLI mode

### Notes on Arguments

- ```--interval```: how long collection runs (minutes)
- ```--poll_pause```: how long to pause between fetch cycles (default: 5 sec)
- ```--max_total```: maximum total posts to fetch (default: 5000)
- ```--overall_timeout```: maximum total time allowed for collection (seconds, default: 400)
- ```--cluster_limit```: max posts to load for clustering (default: 2000)
- ```--n_clusters```: number of KMeans clusters (default: 5)
- If you want to customize these values, just pass the corresponding flag in your command, e.g.:  
  
```bash
$ python main.py --subreddit tech --interval 3 --max_total 800 --cluster_limit 500 --n_clusters 8
```

### 3.2 CLI Mode

After processing, you’ll see:

```bash
Enter keyword/message | 'resume' | 'exit':
```

- Type a keyword (e.g., ```AI```) → see closest cluster info
- Type ```resume``` → start the next collection window
- Type ```exit``` → stop the program

### 3.3 Visualization

After clustering, the program generates: ```clusters.png```

- Each point = a post
- Colors = clusters
- Legend shows cluster IDs