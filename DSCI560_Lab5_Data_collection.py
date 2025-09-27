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

# PRAW - Updated with better rate limiting configuration
def praw_client():
    return praw.Reddit(
        client_id=os.getenv("PRAW_CLIENT_ID"),
        client_secret=os.getenv("PRAW_CLIENT_SECRET"),
        user_agent=os.getenv("PRAW_USER_AGENT", "lab5-dscraper/1.0"),
        ratelimit_seconds=60,  # Increased from 5 to 60 to handle longer waits
    )

# Preprocessing functions remain unchanged
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

def fetch_stream(subreddit: str, total_limit: int, per_batch: int = 100,
                 batch_timeout: int = 60, overall_timeout: int = 400) -> Iterator[Dict]:
    """
    Improved fetch_stream with proper rate limit handling
    
    Key changes:
    1. Reduced per_batch from 1000 to 100 (Reddit API limit per request)
    2. Added proper request counting and rate limiting
    3. Multiple sorting strategies to get more diverse data
    4. Better error handling for rate limits
    """
    reddit = praw_client()
    sub = reddit.subreddit(subreddit)
    got = 0
    overall_start = time.time()
    
    # Track requests to stay within 100 QPM limit
    request_count = 0
    request_start_time = time.time()
    
    print(f"[INFO] Starting collection from r/{subreddit}, target: {total_limit} posts")
    print(f"[INFO] Reddit API limit: 100 requests per minute")
    
    # Use multiple sorting methods to get diverse data
    sorting_methods = [
        ("new", lambda params: sub.new(limit=per_batch, params=params)),
        ("hot", lambda params: sub.hot(limit=per_batch, params=params)),
        ("top_week", lambda params: sub.top(time_filter='week', limit=per_batch, params=params)),
        ("top_month", lambda params: sub.top(time_filter='month', limit=per_batch, params=params))
    ]
    
    after_name = None
    
    for sort_name, sort_func in sorting_methods:
        if got >= total_limit or (time.time() - overall_start) > overall_timeout:
            break
            
        print(f"[INFO] Using {sort_name} sorting method...")
        
        while got < total_limit and (time.time() - overall_start) <= overall_timeout:
            batch_start = time.time()
            batch_count = 0
            
            # Rate limiting: Check if we need to wait
            current_time = time.time()
            if current_time - request_start_time >= 60:
                # Reset counter every minute
                request_count = 0
                request_start_time = current_time
            elif request_count >= 95:  # Leave some buffer under 100 QPM
                wait_time = 60 - (current_time - request_start_time)
                if wait_time > 0:
                    print(f"[INFO] Rate limit approaching, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    request_count = 0
                    request_start_time = time.time()
            
            try:
                # Prepare params for pagination
                params = {"after": after_name} if after_name else None
                
                # Make API request
                posts = sort_func(params)
                request_count += 1
                
                # Convert generator to list to avoid multiple API calls
                batch_posts = list(posts)
                
                if not batch_posts:
                    print(f"[INFO] No more posts available with {sort_name} method")
                    break
                
                for s in batch_posts:
                    if got >= total_limit:
                        break
                    if (time.time() - batch_start) > batch_timeout:
                        print(f"[WARN] Batch timeout reached after {batch_timeout}s")
                        break

                    title, body = s.title or "", s.selftext or ""
                    raw = f"{title} {body}"
                    ct = clean_text(raw)
                    
                    # Skip very short content
                    if len(ct.strip()) < 10:
                        continue
                    
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
                    batch_count += 1
                    after_name = f"t3_{s.id}"
                    
                    # Progress reporting
                    if got % 50 == 0:
                        elapsed = int(time.time() - overall_start)
                        rate = got / (elapsed / 60) if elapsed > 0 else 0
                        print(f"[INFO] Collected {got}/{total_limit} posts, "
                              f"rate: {rate:.1f} posts/min, requests: {request_count}/100")
                
                if batch_count == 0:
                    print(f"[INFO] No new posts in this batch with {sort_name}")
                    break
                
                # Small delay between batches to be respectful
                time.sleep(2)
                
            except Exception as e:
                print(f"[ERROR] Error with {sort_name} method: {e}")
                if "429" in str(e) or "rate" in str(e).lower():
                    print(f"[WARN] Rate limit hit, waiting 60 seconds...")
                    time.sleep(60)
                    request_count = 0
                    request_start_time = time.time()
                break
        
        # Switch to next sorting method
        after_name = None  # Reset pagination for new sorting method
        
        # Brief pause between sorting methods
        if got < total_limit:
            print(f"[INFO] Switching to next sorting method, brief pause...")
            time.sleep(5)

    elapsed = int(time.time() - overall_start)
    print(f"[INFO] Collection finished: {got} posts in {elapsed} seconds")
    print(f"[INFO] Average rate: {got / (elapsed / 60):.1f} posts per minute")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subreddit", required=True, help="e.g., tech or cybersecurity")
    ap.add_argument("--limit", type=int, required=True, help="total posts to fetch (e.g., 1500)")
    ap.add_argument("--overall-timeout", type=int, default=400, help="maximum seconds to run")
    args = ap.parse_args()

    conn = db_conn()
    cur = conn.cursor()

    inserted = 0
    skipped_ads = 0
    errors = 0
    
    try:
        for rec in fetch_stream(args.subreddit, args.limit, 
                               per_batch=100,  # Reduced from 1000 to respect API limits
                               batch_timeout=60, 
                               overall_timeout=args.overall_timeout):
            
            if rec["is_ad"]:
                skipped_ads += 1
                continue
                
            try:
                cur.execute(UPSERT_SQL, (
                    rec["platform_id"], rec["subreddit"], rec["author_mask"], rec["title"], rec["selftext"],
                    rec["created_utc"], rec["url"], rec["is_ad"], json.dumps(rec["keywords"]), rec["clean_text"]
                ))
                inserted += 1
                
                if inserted % 100 == 0:
                    print(f"[INFO] Upserted {inserted} rows (skipped {skipped_ads} ads)...")
                    
            except mysql.Error as e:
                errors += 1
                if "Duplicate entry" not in str(e):
                    print(f"[ERROR] Database error: {e}")
                    
        print(f"[INFO] Final results:")
        print(f"  - Inserted/Updated: {inserted}")
        print(f"  - Skipped ads: {skipped_ads}")
        print(f"  - Database errors: {errors}")
        
    except KeyboardInterrupt:
        print(f"\n[INFO] Collection interrupted by user")
        print(f"[INFO] Partial results: {inserted} inserted, {skipped_ads} ads skipped")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()