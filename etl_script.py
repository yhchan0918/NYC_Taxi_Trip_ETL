import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
from dateutil import relativedelta
from loguru import logger
from psycopg2 import sql
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import time
import os

TRIPS_TABLE_NAME = "trips"
PICKUP_DATETIME_COL = "tpep_pickup_datetime"
db_config = {
    "database": "test_db",
    "user": "admin",
    "password": "root",
    "host": "localhost",
    "port": "5432",
}
LIMIT = 3


def build_url(filename):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"


def build_connection():
    conn = psycopg2.connect(
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
    )
    logger.info("Connection Successful to PostgreSQL")
    return conn


def check_table_exists(cur, table_name):
    # Check if the table exists
    result = cur.execute(
        """
           SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
            ) AS table_existence;
            """,
        [table_name],
    )
    result = cur.fetchall()
    return bool(result[0][0])


def initialize():
    # Connect with postgressql
    logger.info("Starting Initialization")
    conn = build_connection()
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    if check_table_exists(cur, TRIPS_TABLE_NAME):
        logger.info(f"Table {TRIPS_TABLE_NAME} exists")
    else:
        # Create table
        command = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
            VendorID INTEGER , 
            tpep_pickup_datetime TIMESTAMP NOT NULL , 
            tpep_dropoff_datetime TIMESTAMP NOT NULL ,
            PASSENGER_COUNT FLOAT, 
            TRIP_DISTANCE FLOAT, 
            RATECODEID FLOAT,
            store_and_fwd_flag TEXT ,
            PULOCATIONID INTEGER , 
            DOLOCATIONID INTEGER , 
            PAYMENT_TYPE INTEGER,
            FARE_AMOUNT FLOAT, 
            EXTRA FLOAT , 
            MTA_TAX FLOAT, 
            TIP_AMOUNT FLOAT, 
            TOLLS_AMOUNT FLOAT, 
            IMPROVEMENT_SURCHARGE FLOAT, 
            TOTAL_AMOUNT FLOAT, 
            CONGESTION_SURCHARGE FLOAT,
            airport_fee TEXT,
            formatted_pickup_date TEXT,
            hour INTEGER,
            month_year TEXT,
            processed_time TIMESTAMP NOT NULL 
            )
        """
        ).format(sql.Identifier(TRIPS_TABLE_NAME))
        cur.execute(command)
        logger.info(f"Initializing {TRIPS_TABLE_NAME} with Jan 2020 data")
        import_data(datetime.datetime(2020, 1, 1), cur)
    logger.info("Ending Initialization")

    conn.commit()
    cur.close()
    conn.close()


def is_data_available(cur, formatted_month_year):
    cur.execute(
        sql.SQL(
            """
            SELECT
                %s in (SELECT DISTINCT month_year FROM {}) as is_stored
        """
        ).format(sql.Identifier(TRIPS_TABLE_NAME)),
        [formatted_month_year],
    )
    return cur.fetchall()[0][0]


def has_exceeded_database_limit(cur):
    cur.execute(
        sql.SQL(
            """
                SELECT
                    COUNT(DISTINCT month_year)
                FROM {}
            """
        ).format(sql.Identifier(TRIPS_TABLE_NAME))
    )
    number_of_months = cur.fetchall()[0][0]
    logger.info(f"Number of month_year: {number_of_months}")
    return number_of_months == LIMIT


def query_data(input_date):
    logger.info(input_date)
    formatted_month_year = input_date.strftime("%Y-%m")
    # Connect with postgressql
    conn = build_connection()
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # Validate data availability & database limit
    if not is_data_available(cur, formatted_month_year):
        logger.info("Data is not available")
        if has_exceeded_database_limit(cur):
            logger.info("Database will exceed limit")
            delete_data(cur)

        import_data(input_date, cur)

    # Query database
    logger.info(f"Querying data")
    cur.execute(
        sql.SQL(
            """
        SELECT
            hour,
            COUNT(*) as trip_count
        FROM {}
        WHERE {}::DATE = %s
        GROUP BY hour
        """
        ).format(sql.Identifier(TRIPS_TABLE_NAME), sql.Identifier(PICKUP_DATETIME_COL)),
        [input_date],
    )
    result = cur.fetchall()
    logger.info(f"Queried result: {result[0] if len(result) > 0 else 'None'}")

    conn.commit()

    cur.close()
    conn.close()
    logger.info("Closed PostgreSQL Connection")
    return result


def delete_data(cur):
    logger.info("Deleting Data")
    cur.execute(
        sql.SQL(
            """
            DELETE FROM {} WHERE processed_time = (SELECT MIN(processed_time) FROM {})
        """
        ).format(sql.Identifier(TRIPS_TABLE_NAME), sql.Identifier(TRIPS_TABLE_NAME))
    )
    logger.info("Finish Deleting")


def import_data(date, cur):
    formatted_month_year = date.strftime("%Y-%m")
    logger.info(f"Starting ETL Script for {formatted_month_year} data...")
    start_date = date.replace(day=1)
    end_date = start_date + relativedelta.relativedelta(months=1)

    filename = f"yellow_tripdata_{formatted_month_year}.parquet"
    dataset_url = build_url(filename)
    # Read file from website
    # Import trip data
    df = pd.read_parquet(dataset_url, engine="pyarrow").iloc[:5]
    # Need to handle data validation
    if not ("PULocationID" in df.columns and "DOLocationID" in df.columns):
        raise Exception("No location data")

    df = df[
        (df[PICKUP_DATETIME_COL] >= start_date) & (df[PICKUP_DATETIME_COL] < end_date)
    ]
    df["formatted_pickup_date"] = df[PICKUP_DATETIME_COL].dt.strftime("%Y-%m-%d")
    df["hour"] = df[PICKUP_DATETIME_COL].dt.hour
    df["month_year"] = formatted_month_year
    df["processed_time"] = datetime.datetime.now()
    # engine = create_engine(
    #     f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}'
    # )
    exported_filename = f"{formatted_month_year}.csv"
    df.to_csv(exported_filename, index=False)
    start_time = time.time()
    logger.info(f"Start Copying {len(df)} rows...")
    with open(exported_filename, "r") as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, TRIPS_TABLE_NAME, sep=",", null="")
    logger.info("Done Copying...")
    os.remove(exported_filename)

    logger.info(f"Ending Etl script... Time taken: {time.time() - start_time}")


initialize()
for year in range(2015, 2022):
    for month in range(1, 13):
        try:
            print(query_data(datetime.datetime(year, month, 12)))
        except Exception as error:
            logger.error(f"{year}-{month}")
            logger.error(error)
