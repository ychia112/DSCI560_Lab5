## 1. Run the database schema
Run the provided schema file to create the database and table.

```bash
mysql -u <YOUR_MYSQL_USER> -p < schema.sql

```

This creates the `reddit_db` database and the `reddit_posts` table.

---

## 2. Create a Reddit app (API credentials)
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

## 3. Create the `.env` file
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

## 4. Run the scraper script
Use the following command:

```bash
python3 DSCI560_Lab5_Data_collection.py --subreddit technology --limit 500
```

The script will:
- Fetch posts with PRAW
- Preprocess text (clean, mask usernames)
- Upsert into MySQL (`ON DUPLICATE KEY UPDATE` ensures no duplicates)

---
