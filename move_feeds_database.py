import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Source (old) database connection
SOURCE_DATABASE_URL = os.getenv(
    "SOURCE_DATABASE_URL",
    "postgresql://postgres.cogggfzsukkoebjaslhf:vSg7KHIXFbqwWeSs@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
)

# Destination (new) database connection
DEST_DATABASE_URL = os.getenv(
    "DEST_DATABASE_URL",
    "postgresql://postgres.yepxgasuunenbcwxlsrp:postgresadmin@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
)

FEEDS_TABLE_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS feeds (
    id SERIAL PRIMARY KEY,
    name TEXT,
    source_url TEXT,
    feed_type TEXT,
    parser_type TEXT,
    company_name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    check_frequency_minutes INTEGER,
    last_checked_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ,
    last_run_status TEXT,
    last_succeeded_at TIMESTAMPTZ,
    consecutive_failure_count INTEGER,
    last_error_message TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    unique_jobs_processed INTEGER,
    cw_auto_approved_jobs INTEGER,
    avg_processing_time INTEGER,
    error_count INTEGER,
    avg_feed_size FLOAT
);
"""

def map_feed_row(old_row):
    # Map old fields to new fields, set defaults for new fields
    return {
        "name": old_row.get("name"),
        "source_url": old_row.get("feed_url"),
        "feed_type": old_row.get("feed_type"),
        "parser_type": old_row.get("parser_type") or "",
        "company_name": old_row.get("company") or "",
        "is_active": old_row.get("status", "active") == "active",
        "check_frequency_minutes": old_row.get("polling_interval"),
        "last_checked_at": old_row.get("last_polled"),
        "next_check_at": None,  # No direct mapping
        "last_run_status": None,  # No direct mapping
        "last_succeeded_at": old_row.get("last_processed_at"),
        "consecutive_failure_count": old_row.get("error_count"),
        "last_error_message": old_row.get("error_message"),
        "created_at": old_row.get("created_at"),
        "updated_at": old_row.get("updated_at"),
        "unique_jobs_processed": old_row.get("processed_jobs_count"),
        "cw_auto_approved_jobs": None,  # No direct mapping
        "avg_processing_time": old_row.get("avg_processing_time"),
        "error_count": old_row.get("error_count"),
        "avg_feed_size": None,  # No direct mapping
    }

def main():
    # Connect to source DB
    src_conn = psycopg2.connect(SOURCE_DATABASE_URL)
    src_cur = src_conn.cursor(cursor_factory=RealDictCursor)

    # Connect to destination DB
    dest_conn = psycopg2.connect(DEST_DATABASE_URL)
    dest_cur = dest_conn.cursor()

    # Create feeds table if it doesn't exist
    dest_cur.execute(FEEDS_TABLE_CREATE_SQL)
    dest_conn.commit()

    # Fetch all feed rows from source
    src_cur.execute("SELECT * FROM feeds")
    old_rows = src_cur.fetchall()

    if not old_rows:
        print("No feeds found in source database.")
        return

    print(f"Found {len(old_rows)} feeds in source database.")

    # Insert each mapped feed into destination
    for old_row in old_rows:
        new_row = map_feed_row(old_row)
        columns = ', '.join(new_row.keys())
        placeholders = ', '.join(['%s'] * len(new_row))
        values = list(new_row.values())

        insert_sql = f"INSERT INTO feeds ({columns}) VALUES ({placeholders})"
        dest_cur.execute(insert_sql, values)

    dest_conn.commit()
    print(f"Inserted {len(old_rows)} feeds into feeds table in destination database.")

    # Close connections
    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

if __name__ == "__main__":
    main()