import psycopg2

# Paste your Supabase connection string here
CONNECTION_STRING = ""


def get_all_feeds():
    # Connect to Supabase/Postgres
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()

    # Get all rows and columns from feeds table
    cur.execute("SELECT * FROM feeds;")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    print("Columns:", columns)
    print("Rows:")
    for row in rows:
        print(dict(zip(columns, row)))

    cur.close()
    conn.close()


if __name__ == "__main__":
    get_all_feeds()