import os
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

def ensure_column(cur, table, column, ddl):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (table, column),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(ddl)

def ensure_index(cur, table, index_name, ddl):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND INDEX_NAME = %s",
        (table, index_name),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(ddl)

def main():
    conn = db_conn()
    cur = conn.cursor()
    ensure_column(cur, 'reddit_posts', 'embedding',
                  "ALTER TABLE reddit_posts ADD COLUMN embedding JSON DEFAULT NULL")
    ensure_column(cur, 'reddit_posts', 'ocr_text',
                  "ALTER TABLE reddit_posts ADD COLUMN ocr_text TEXT DEFAULT NULL")
    ensure_index(cur, 'reddit_posts', 'idx_created_utc',
                 "ALTER TABLE reddit_posts ADD INDEX idx_created_utc (created_utc)")
    cur.close()
    conn.close()
    print("Migration checks/executions done.")

if __name__ == "__main__":
    main()