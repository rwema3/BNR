import json
import logging
import polars as pl
from clickhouse_connect import get_client

# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------
# Configure logging to display INFO level messages (and above) with timestamps.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -------------------------------------------------------------------
# Minimal dbutils wrapper for ClickHouse
# -------------------------------------------------------------------
class DBUtils:
    """
    A minimal utility class to interact with a ClickHouse database.
    Provides methods to insert rows, run queries, and execute commands.
    """
    def __init__(self, host, port, username, password, database):
        # Establish connection with ClickHouse using clickhouse-connect client
        self.client = get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )

    def insert(self, table, rows, columns):
        """
        Insert rows into a given table.
        :param table: str, name of the ClickHouse table
        :param rows: list of tuples/lists, rows to insert
        :param columns: list of column names in order
        """
        return self.client.insert(table, rows, column_names=columns)

    def query(self, sql):
        """
        Execute a SQL SELECT query and return the rows.
        """
        return self.client.query(sql).result_rows

    def command(self, sql):
        """
        Execute a SQL command (e.g., CREATE, DROP, ALTER).
        """
        return self.client.command(sql)


# -------------------------------------------------------------------
# Connect to ClickHouse
# -------------------------------------------------------------------
# Initialize DB connection with provided credentials
db = DBUtils(
    host='3.143.170.120',
    port=8123,
    username='analytic',
    password='rwema123!',
    database='amazon'
)

# -------------------------------------------------------------------
# Drop and create table with deduplication support
# -------------------------------------------------------------------
logging.info("Dropping table 'reviews' if it exists...")
db.command("DROP TABLE IF EXISTS reviews")

logging.info("Creating table 'reviews'...")
# The table uses ReplacingMergeTree engine to support deduplication
db.command("""
CREATE TABLE reviews (
    rating Float32,
    title String,
    text String,
    images String,
    asin String,
    parent_asin String,
    user_id String,
    timestamp DateTime64(3),
    helpful_vote UInt32,
    verified_purchase UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY (asin, user_id, timestamp)
""")
logging.info("Table 'reviews' created successfully.")

# -------------------------------------------------------------------
# Prepare ingestion
# -------------------------------------------------------------------
BATCH_SIZE = 500  # Number of rows to buffer before inserting
total_inserted = 0  # Counter for successfully inserted rows
COLUMN_ORDER = [
    "rating", "title", "text", "images", "asin",
    "parent_asin", "user_id", "timestamp", "helpful_vote", "verified_purchase"
]

def prepare_record(record):
    """
    Transform a JSON record into a structured dictionary ready for insertion.
    Ensures correct types, serializes lists as JSON strings, and sets defaults.
    """
    return {
        "rating": float(record.get("rating", 0.0)),
        "title": str(record.get("title", "")),
        "text": str(record.get("text", "")),
        "images": json.dumps(record.get("images", [])),  # serialize list of images
        "asin": str(record.get("asin", "")),
        "parent_asin": str(record.get("parent_asin", "")),
        "user_id": str(record.get("user_id", "")),
        "timestamp": int(record.get("timestamp", 0)),  # store as epoch milliseconds
        "helpful_vote": int(record.get("helpful_vote", 0)),
        "verified_purchase": 1 if record.get("verified_purchase", False) else 0
    }

# -------------------------------------------------------------------
# Start ingestion
# -------------------------------------------------------------------
logging.info("Starting data ingestion...")

with open("Subscription_Boxes.jsonl", "r", encoding="utf-8") as f:
    batch = []  # temporary storage for buffered rows
    for line_number, line in enumerate(f, 1):
        if line.strip():  # skip empty lines
            batch.append(prepare_record(json.loads(line)))

        # Process batch once it reaches the threshold
        if len(batch) >= BATCH_SIZE:
            # Deduplication: check if these keys already exist in DB
            keys = [(r["asin"], r["user_id"], r["timestamp"]) for r in batch]

            existing = []
            if keys:  # Avoid executing empty IN clause
                in_clause = ",".join(
                    f"('{asin}', '{user_id}', {timestamp})"
                    for asin, user_id, timestamp in keys
                )
                sql = f"""
                SELECT asin, user_id, timestamp
                FROM reviews
                WHERE (asin, user_id, timestamp) IN ({in_clause})
                """
                existing = db.query(sql)

            existing_set = set(existing)
            # Keep only new rows that are not already in DB
            new_batch = [r for r in batch if (r["asin"], r["user_id"], r["timestamp"]) not in existing_set]

            if new_batch:
                # Convert new rows into Polars DataFrame, keep column order
                df = pl.DataFrame(new_batch)[COLUMN_ORDER]
                try:
                    # Insert rows into ClickHouse
                    db.insert("reviews", df.rows(), COLUMN_ORDER)
                    total_inserted += len(new_batch)
                    logging.info(f"Inserted batch ending at line {line_number}, total inserted: {total_inserted}")
                except Exception as e:
                    logging.error(f"Error inserting batch at line {line_number}: {e}")
            else:
                logging.info(f"Skipped batch ending at line {line_number}, all rows already exist")

            # Reset batch buffer
            batch = []

    # -------------------------------------------------------------------
    # Insert any remaining rows after file read completes
    # -------------------------------------------------------------------
    if batch:
        keys = [(r["asin"], r["user_id"], r["timestamp"]) for r in batch]

        existing = []
        if keys:
            in_clause = ",".join(
                f"('{asin}', '{user_id}', {timestamp})"
                for asin, user_id, timestamp in keys
            )
            sql = f"""
            SELECT asin, user_id, timestamp
            FROM reviews
            WHERE (asin, user_id, timestamp) IN ({in_clause})
            """
            existing = db.query(sql)

        existing_set = set(existing)
        new_batch = [r for r in batch if (r["asin"], r["user_id"], r["timestamp"]) not in existing_set]

        if new_batch:
            df = pl.DataFrame(new_batch)[COLUMN_ORDER]
            try:
                db.insert("reviews", df.rows(), COLUMN_ORDER)
                total_inserted += len(new_batch)
                logging.info(f"Inserted final batch, total inserted: {total_inserted}")
            except Exception as e:
                logging.error(f"Error inserting final batch: {e}")
        else:
            logging.info("Skipped final batch, all rows already exist")

logging.info(f"Data ingestion finished. Total rows inserted: {total_inserted}")





