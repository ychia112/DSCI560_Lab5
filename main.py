import os
import time
import json
import numpy as np
import argparse

#  Import data collection code
from DSCI560_Lab5_Data_collection import fetch_stream, db_conn, UPSERT_SQL

#  Import data preprocessing
from preprocessing import RedditPreprocessor

# Database helper functions
from db_utils import load_from_db, save_clusters_to_db, save_cluster_metadata

# Import clustering
from clustering import cluster_messages, get_representative_posts, visualize_clusters


def collection_phase(subreddit, duration_min=5, poll_pause=5, max_total=5000, overall_timeout=400):
    conn = db_conn()
    cur = conn.cursor()
    start = time.time()
    pulls = 0
    print(f"[INFO] Collecting from r/{subreddit} for {duration_min} min...")
    
    while True:
        # Stop Conditions
        if (time.time() - start) >= duration_min * 60:
            break
        if (time.time() - start) >= overall_timeout:
            break
        if pulls >= max_total:
            break
        for rec in fetch_stream(subreddit, total_limit=max_total, overall_timeout=overall_timeout):
            if rec["is_ad"]:
                continue
            
            cur.execute(UPSERT_SQL, (
                rec["platform_id"], rec["subreddit"], rec["author_mask"], rec["title"],
                rec["selftext"], rec["created_utc"], rec["url"], rec["is_ad"],
                json.dumps(rec["keywords"]), rec["clean_text"]
            ))
            pulls += 1

            # check limitations again
            if pulls >= max_total:
                break
            if (time.time() - start) >= duration_min * 60:
                break
            if (time.time() - start) >= overall_timeout:
                break

        print(f"[INFO] Pulled {pulls} posts so far...")
        time.sleep(poll_pause)
                
    elapsed = int(time.time() - start)
    print(f"[INFO] Collection finished: {pulls} posts in {elapsed} sec.")
    
    print("[INFO] Committing changes to the database...")
    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Database connection closed.")
        

                
# Embedding
def embedding_phase(batch_size=50):
    print("[INFO] Generating embeddings...")
    pre = RedditPreprocessor()
    pre.process_all_posts(batch_size=batch_size)
    print("[INFO] Embeddings saved to DB.")
            

# Clustering + Visualization
def processing_phase(cluster_limit=2000, n_clusters=5):
    ids, messages, embeddings = load_from_db(limit=cluster_limit)
    triples = [(i, m, e) for i, m, e in zip(ids, messages, embeddings) if e]
    if not triples:
        print("[WARN] No embeddings found, skip clustering.")
        return None, None, None
    
    ids_f, msgs_f, embs_f = zip(*triples)
    X = np.array(embs_f, dtype=float)
    
    labels, keywords = cluster_messages(X, list(msgs_f), n_clusters=n_clusters)
    reps = get_representative_posts(X, labels, list(ids_f))
    
    save_clusters_to_db(list(ids_f), list(labels))
    save_cluster_metadata(keywords, reps)
    
    visualize_clusters(X, labels, keywords)
    print("[INFO] Processing done (cluster.png saved)")
    return labels, keywords, reps, list(ids_f), list(msgs_f)
    
    
def cli_mode(labels, keywords, reps, ids=None, messages=None):
    while True:
        user_input = input("\nEnter keyword/message | 'resume' | 'exit': ").strip()
        if user_input.lower() == 'exit':
            return "exit"
        if user_input.lower() == 'resume':
            return 'resume'
        if labels is None:
            print("No cluster available yet, type 'resume' to collect.")
            continue
        
        # Search
        found = None
        for cid, kws in (keywords or {}).items():
            if any(k.lower() in user_input.lower() or user_input.lower() in k.lower() for k in kws):
                found = cid
                break
            
        if found is not None:
            print(f"\n[HIT] Cluster: {found}")
            print(f"Keywords: {keywords.get(found, [])}")
            
            # show representative message/post
            rep_id = reps.get(found)
            rep_msg = None
            if ids and messages and rep_id in ids:
                idx = ids.index(rep_id)
                rep_msg = messages[idx]
            print(f"Representative post: {rep_msg if rep_msg else rep_id}")
            
            # Show some messages in this cluster
            cluster_msgs = [m for i, m in zip(ids, messages) if labels[ids.index(i)] == found]
            print("Cluster messages (sample):")
            for sample in cluster_msgs[:5]:
                print(" -", sample[:120], "..." if len(sample) > 120 else "")
            
            print("\nSee cluster.png for visualization")
        else:
            print("[MISS] No matching cluster found.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--subreddit", required=True,  help="e.g., tech or cybersecurity")
    parser.add_argument("--interval", type=int, help="Collection window (minutes)")
    parser.add_argument("--poll_pause", type=int, default=5)
    parser.add_argument("--max_total", type=int, default=5000, help="Maximum total posts to fetch")
    parser.add_argument("--overall_timeout", type=int, default=400, help="Maximum seconds to run collection")
    parser.add_argument("--cluster_limit", type=int, default=2000)
    parser.add_argument("--n_clusters", type=int, default=5)
    args = parser.parse_args()
    
    while True:
        # 1) Collecting new posts
        collection_phase(
            args.subreddit, 
            duration_min=args.interval, 
            max_total=args.max_total, 
            overall_timeout=args.overall_timeout
        )
        # 2) Run embedding
        embedding_phase(batch_size=50)
        
        # 3) 3) Run clustering + visualization
        labels, keywords, reps, ids, messages = processing_phase(cluster_limit=args.cluster_limit, n_clusters=args.n_clusters)
        
        # 4) CLI
        action = cli_mode(labels, keywords, reps, ids, messages)
        if action == 'exit':
            print("[INFO] Exiting program.")
            break

if __name__ == '__main__':
    main()