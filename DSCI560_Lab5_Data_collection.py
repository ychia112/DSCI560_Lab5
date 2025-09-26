import os, time, json, re, argparse, datetime as dt
from typing import Iterator, Dict, List
import praw
from dotenv import load_dotenv
import mysql.connector as mysql

load_dotenv()
def db_conn():
    return mysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB", "reddit_db"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        autocommit=True,
    )

UPSERT_SQL = """
INSERT INTO reddit_posts
(platform_id, subreddit, author_mask, title, selftext, created_utc, url, is_ad, keywords, clean_text)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  title=VALUES(title),
  selftext=VALUES(selftext),
  clean_text=VALUES(clean_text),
  is_ad=VALUES(is_ad),
  keywords=VALUES(keywords)
"""

# PRAW
def praw_client():
    return praw.Reddit(
        client_id=os.getenv("PRAW_CLIENT_ID"),
        client_secret=os.getenv("PRAW_CLIENT_SECRET"),
        user_agent=os.getenv("PRAW_USER_AGENT", "lab5-dscraper/1.0"),
        ratelimit_seconds=5,
    )

# Preprocessing data: Remove specials/irrelevant, mask usernames, convert time
_URL = re.compile(r"http\S+")
_USER = re.compile(r"u/[A-Za-z0-9_-]+")
_NONWORD = re.compile(r"[^a-z0-9\s]+")

def clean_text(s: str) -> str:
    s = (s or "").lower()
    s = _URL.sub(" ", s)
    s = _USER.sub("u_user", s)
    s = _NONWORD.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_ad(post) -> bool:
    flair = (post.link_flair_text or "").lower()
    return "promo" in flair or "ad" in flair or post.stickied

def top_keywords_simple(text: str, top_k: int = 10) -> List[str]:
    toks = [w for w in text.split() if len(w) > 3]
    freq = {}
    for w in toks:
        freq[w] = freq.get(w, 0) + 1
    return [w for w, _ in sorted(freq.items(), key=lambda x: x[1], reverse=True)[:top_k]]

def fetch_stream(subreddit: str, total_limit: int, per_batch: int = 1000,
                 batch_timeout: int = 60, overall_timeout: int = 400) -> Iterator[Dict]:
    reddit = praw_client()
    sub = reddit.subreddit(subreddit)
    got = 0
    overall_start = time.time()
    after_name = None

    while got < total_limit and (time.time() - overall_start) <= overall_timeout:
        batch_start = time.time()
        count_this = 0
        gen = sub.new(limit=None, params={"after": after_name} if after_name else None)
        for s in gen:
            if got >= total_limit:
                break
            if count_this >= per_batch or (time.time() - batch_start) > batch_timeout:
                break

            title, body = s.title or "", s.selftext or ""
            raw = f"{title} {body}"
            ct = clean_text(raw)
            rec = {
                "platform_id": s.id,
                "subreddit": str(s.subreddit),
                "author_mask": "u_user",
                "title": title,
                "selftext": body,
                "created_utc": dt.datetime.fromtimestamp(s.created_utc, dt.timezone.utc),
                "url": f"https://www.reddit.com{s.permalink}",
                "is_ad": looks_like_ad(s),
                "clean_text": ct,
                "keywords": top_keywords_simple(ct, top_k=10),
            }
            yield rec
            got += 1
            count_this += 1
            after_name = f"t3_{s.id}"
        if count_this == 0:
            break
        time.sleep(1.5)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subreddit", required=True, help="e.g., tech or cybersecurity")
    ap.add_argument("--limit", type=int, required=True, help="total posts to fetch (e.g., 1500)")
    ap.add_argument("--overall-timeout", type=int, default=400)
    args = ap.parse_args()

    conn = db_conn()
    cur = conn.cursor()

    inserted = 0
    for rec in fetch_stream(args.subreddit, args.limit, per_batch=1000, batch_timeout=60, overall_timeout=args.overall_timeout):
        if rec["is_ad"]:
            continue
        cur.execute(UPSERT_SQL, (
            rec["platform_id"], rec["subreddit"], rec["author_mask"], rec["title"], rec["selftext"],
            rec["created_utc"], rec["url"], rec["is_ad"], json.dumps(rec["keywords"]), rec["clean_text"]
        ))
        inserted += 1
        if inserted % 100 == 0:
            print(f"Upserted {inserted} rows...")
    print(f"Upserted total rows: {inserted}")

if __name__ == "__main__":
    main()
