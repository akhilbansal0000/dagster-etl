import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dagster import asset, repository, job, op, Out, In, get_dagster_logger
from dagster import Definitions, in_process_executor

# Paths
DATA_DIR = "/opt/dagster/dagster_home/data"

# Logger for debugging
logger = get_dagster_logger()


def get_postgres_connection():
    return psycopg2.connect(
        dbname="dagster",
        user="dagster",
        password="dagster",
        host="postgres",
        port=5432
    )


@op(out=Out(pd.DataFrame))
def load_order_details():
    """Load Order Details CSV"""
    file_path = os.path.join(DATA_DIR, "Order Details.csv")
    logger.info(f"Loading data from {file_path}")
    
    dtype_map = {
        "Order ID": str,
        "Amount": "float32",
        "Profit": "float32",
        "Quantity": "int16",
        "Category": "category",
        "Sub-Category": "category",
    }

    df = pd.read_csv(file_path, dtype=dtype_map)
    logger.info(f"Loaded {df.shape[0]} rows from Order Details")

    print(df.head(5))
    
    return df


@op(out=Out(pd.DataFrame))
def load_list_orders():
    """Load List of Orders CSV"""
    file_path = os.path.join(DATA_DIR, "List of Orders.csv")
    logger.info(f"Loading data from {file_path}")
    
    dtype_map = {
        "Order ID": str,
        "Order Date": str,  # Keep as string initially
        "CustomerName": str,
        "City": str,
        "State": str,
    }

    df = pd.read_csv(file_path, dtype=dtype_map)

    # Convert Order Date to datetime
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")

    # Check if conversion worked
    if df["Order Date"].isna().any():
        logger.error("Some Order Date values could not be converted. Check the data format.")
        raise ValueError("Invalid date format found in Order Date column.")

    # Generate hourly timestamps
    df["Order Date"] = df["Order Date"].dt.strftime('%Y-%m-%d') + " " + (df.index % 24).astype(str) + ":00:00"
    df["Order Date"] = pd.to_datetime(df["Order Date"])  # Convert back to datetime

    logger.info(f"Loaded {df.shape[0]} rows from List of Orders")

    print(df.head(5))
    
    return df


@op(ins={"order_details": In(pd.DataFrame), "list_orders": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def transform_data(order_details, list_orders):
    """Merge Order Details and Order List on 'Order ID'"""
    logger.info("Transforming data by merging order details and customer data")
    
    merged_df = order_details.merge(list_orders, on="Order ID", how="left")
    
    # Convert "Order Date" to TIMESTAMP format for partitioning
    merged_df["Order Date"] = pd.to_datetime(merged_df["Order Date"])
    
    logger.info(f"Transformed dataset has {merged_df.shape[0]} rows")
    
    return merged_df


@op(ins={"transformed_data": In(pd.DataFrame)})
def store_data_in_db(transformed_data):
    """Store transformed data into PostgreSQL with partitions"""
    logger.info("Storing data in PostgreSQL...")

    conn = get_postgres_connection()
    cursor = conn.cursor()

    # Create the main table with partitioning
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT,
            order_date TIMESTAMP NOT NULL,
            customer_name TEXT,
            city TEXT,
            state TEXT,
            amount FLOAT,
            profit FLOAT,
            quantity INT,
            category TEXT,
            sub_category TEXT,
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (order_id, order_date)
        ) PARTITION BY RANGE (order_date);
    """)

    # Extract unique order dates
    unique_dates = transformed_data["Order Date"].dt.date.unique()
    
    for date in unique_dates:
        partition_name = f"orders_{date.strftime('%Y_%m_%d')}"
        start_time = f"'{date} 00:00:00'"
        end_time = f"'{date + pd.Timedelta(days=1)} 00:00:00'"

        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} PARTITION OF orders
            FOR VALUES FROM ({}) TO ({});
        """).format(sql.Identifier(partition_name), sql.Literal(start_time), sql.Literal(end_time)))

    # Insert data
    for _, row in transformed_data.iterrows():
        cursor.execute("""
            INSERT INTO orders (order_id, order_date, customer_name, city, state, amount, profit, quantity, category, sub_category, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (order_id, order_date)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                amount = EXCLUDED.amount,
                profit = EXCLUDED.profit,
                quantity = EXCLUDED.quantity,
                category = EXCLUDED.category,
                sub_category = EXCLUDED.sub_category,
                updated_at = NOW();
        """, (
            row["Order ID"], row["Order Date"], row["CustomerName"],
            row["City"], row["State"], row["Amount"], row["Profit"],
            row["Quantity"], row["Category"], row["Sub-Category"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Data successfully stored in PostgreSQL")


@job(executor_def=in_process_executor)
def etl_job():
    """Defines the full ETL pipeline"""
    order_details = load_order_details()
    list_orders = load_list_orders()
    transformed_data = transform_data(order_details, list_orders)
    store_data_in_db(transformed_data)


@repository
def dagster_etl():
    """Dagster repository containing all jobs and assets"""
    return [etl_job]


# Expose the definitions (Dagster 1.0+)
defs = Definitions(jobs=[etl_job])
