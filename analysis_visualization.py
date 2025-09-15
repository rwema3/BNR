# --------------------------------------------------------
# Task 2: Analysis & Visualization using clickhouse_connect + Polars
# --------------------------------------------------------
import polars as pl
import matplotlib.pyplot as plt
from clickhouse_connect import get_client

# -----------------------------
# Connect to ClickHouse
# -----------------------------
# Establish a connection to the ClickHouse database.
# The connection is configured with host, port, and credentials.
client = get_client(
    host="3.143.170.120",
    port=8123,
    username="analytic",
    password="rwema123!",
    database="amazon"
)

# -----------------------------
# 1. Top 10 products by number of reviews
# -----------------------------
# Query products with the highest number of reviews.
# Also calculate the average rating for each product.
sql_top_products = """
SELECT asin, count() AS reviews, round(avg(rating),2) AS avg_rating
FROM reviews
GROUP BY asin
ORDER BY reviews DESC
LIMIT 10
"""
rows = client.query(sql_top_products).result_rows
df_top_products = pl.DataFrame(rows, schema=["asin", "reviews", "avg_rating"])

print("Top 10 Products by Number of Reviews:")
print(df_top_products)

# -----------------------------
# 2. Rating distribution
# -----------------------------
# Query the distribution of ratings (1–5).
# Visualize it as a bar chart using Matplotlib.
sql_rating = "SELECT rating, count() AS cnt FROM reviews GROUP BY rating ORDER BY rating"
rows = client.query(sql_rating).result_rows
df_rating = pl.DataFrame(rows, schema=["rating", "cnt"])

plt.figure(figsize=(8, 5))
plt.bar(df_rating["rating"], df_rating["cnt"], color="skyblue")
plt.xlabel("Rating")
plt.ylabel("Number of Reviews")
plt.title("Rating Distribution")
plt.show()

# -----------------------------
# 3. Verified purchase percentage
# -----------------------------
# Calculate the percentage of reviews that come from verified purchases.
sql_verified = "SELECT round(100 * sum(verified_purchase)/count(),2) AS pct_verified FROM reviews"
pct_verified = client.query(sql_verified).result_rows[0][0]

print(f"Percentage of Verified Purchases: {pct_verified}%")

# -----------------------------
# 4. Reviews over time (monthly)
# -----------------------------
# Count reviews aggregated by month.
# Visualize the trend using a line chart.
sql_time = """
SELECT toStartOfMonth(timestamp) AS month, count() AS reviews
FROM reviews
GROUP BY month
ORDER BY month
"""
rows = client.query(sql_time).result_rows
df_time = pl.DataFrame(rows, schema=["month", "reviews"])

plt.figure(figsize=(10, 5))
plt.plot(df_time["month"], df_time["reviews"], marker="o", color="orange")
plt.xlabel("Month")
plt.ylabel("Number of Reviews")
plt.title("Reviews Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# -----------------------------
# 5. Top 10 reviewers by number of reviews
# -----------------------------
# Identify the most active reviewers based on review count.
sql_top_users = """
SELECT user_id, count() AS reviews
FROM reviews
GROUP BY user_id
ORDER BY reviews DESC
LIMIT 10
"""
rows = client.query(sql_top_users).result_rows
df_top_users = pl.DataFrame(rows, schema=["user_id", "reviews"])

print("Top 10 Reviewers by Number of Reviews:")
print(df_top_users)
