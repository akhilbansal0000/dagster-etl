import os
import pandas as pd
import sqlite3
from dagster import asset, repository, job, op, Out, In, get_dagster_logger
from dagster import Definitions
from dagster import in_process_executor

# Define paths for input and output
DATA_DIR = "/opt/dagster/dagster_home/data"
DB_PATH = "/opt/dagster/dagster_home/storage/orders.db"

# Logger for debugging
logger = get_dagster_logger()

# Ensure storage directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


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
    
    return df


@op(out=Out(pd.DataFrame))
def load_list_orders():
    """Load List of Orders CSV"""
    file_path = os.path.join(DATA_DIR, "List of Orders.csv")
    logger.info(f"Loading data from {file_path}")
    
    dtype_map = {
        "Order ID": str,
        "Order Date": str,
        "CustomerName": str,
        "City": str,
        "State": str,
    }

    df = pd.read_csv(file_path, dtype=dtype_map, parse_dates=["Order Date"])
    logger.info(f"Loaded {df.shape[0]} rows from List of Orders")
    
    return df


@op(ins={"order_details": In(pd.DataFrame), "list_orders": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def transform_data(order_details, list_orders):
    """Merge Order Details and Order List on 'Order ID'"""
    logger.info("Transforming data by merging order details and customer data")
    
    merged_df = order_details.merge(list_orders, on="Order ID", how="left")
    logger.info(f"Transformed dataset has {merged_df.shape[0]} rows")
    
    return merged_df


@op(ins={"transformed_data": In(pd.DataFrame)})
def store_data_in_db(transformed_data):
    """Store transformed data into SQLite database"""
    logger.info(f"Storing data in SQLite at {DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        transformed_data.to_sql("orders", conn, if_exists="replace", index=False)
        logger.info("Data successfully stored in SQLite")


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
