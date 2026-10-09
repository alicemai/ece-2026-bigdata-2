import os

import duckdb

QUERIES = {
    "1. avrg orders per user and average quantity per order": """
        SELECT
          round(count(*) * 1.0 / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
          round(avg(quantity), 2) AS avg_quantity_per_order
        FROM orders
    """,
    "2. first and last order of each user, and days between them": """
        SELECT
          u.username,
          min(o.date) AS first_order,
          max(o.date) AS last_order,
          date_diff('day', min(o.date), max(o.date)) AS days_between
        FROM orders o
        JOIN users u ON o.user_uuid = u.uuid
        GROUP BY ALL
        ORDER BY u.username
    """,
    "3. hr of the day with the highest quantity sold": """
        SELECT hour(date) AS hour, sum(quantity) AS quantity
        FROM orders
        GROUP BY hour
        ORDER BY quantity DESC
        LIMIT 1
    """,
    "4. month-over-month variation of quantity per product (%)": """
        WITH monthly AS (
          SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
          FROM orders
          GROUP BY month, product
        )
        SELECT
          month,
          product,
          quantity,
          round(
            100 * (quantity - lag(quantity) OVER (PARTITION BY product ORDER BY month))
            / lag(quantity) OVER (PARTITION BY product ORDER BY month),
            1
          ) AS variation_pct
        FROM monthly
        ORDER BY product, month
    """,
    "5. users who ordered every product at least once": """
        SELECT u.username, u.name
        FROM orders o
        JOIN users u ON o.user_uuid = u.uuid
        GROUP BY ALL
        HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
    """,
}


def main():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    bronze = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"
    con.execute(f"CREATE TABLE users AS FROM read_csv('{bronze}/users.csv', strict_mode = false)")
    con.execute(f"CREATE TABLE orders AS FROM read_csv('{bronze}/orders.csv', strict_mode = false)")
    for title, query in QUERIES.items():
        print(f"\n== {title}")
        print(con.sql(query))


if __name__ == "__main__":
    main()