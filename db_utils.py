import os
import json
import mysql.connector as mysql
from dotenv import load_dotenv

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


def load_from_db(limit=2000):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, clean_text, embedding FROM reddit_posts "
        "WHERE embedding IS NOT NULL LIMIT %s", (limit,)
    )
    rows = cur.fetchall()
    ids, messages, embeddings = [], [], []
    for rid, msg, emb in rows:
        ids.append(rid)
        messages.append(msg)
        embeddings.append(json.loads(emb) if emb else None)
    return ids, messages, embeddings


def save_clusters_to_db(post_ids, labels):
    conn = db_conn()
    cur = conn.cursor()
    for pid, lid in zip(post_ids, labels):
        cur.execute(
            "UPDATE reddit_posts SET cluster_id = %s WHERE id = %s",
            (int(lid), int(pid))
        )
    conn.commit()
    print(f"[INFO] Updated {len(post_ids)} posts with cluster assignments")

    
def save_cluster_metadata(keywords_dict, reps_dict):
    conn = db_conn()
    cur = conn.cursor()
    for cid, kws in keywords_dict.items():
        rep_post = reps_dict.get(cid)
        cur.execute(
            """
            INSERT INTO cluster_metadata (cluster_id, keywords, representative_post_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              keywords = VALUES(keywords),
              representative_post_id = VALUES(representative_post_id)
            """,
            (int(cid), json.dumps(kws), int(rep_post) if rep_post else None)
        )
    conn.commit()
    print(f"[INFO] Saved metadata for {len(keywords_dict)} clusters")