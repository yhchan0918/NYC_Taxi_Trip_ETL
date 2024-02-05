import pandas as pd
import psycopg2
import datetime
from dateutil import relativedelta
from loguru import logger
from psycopg2 import sql
import time
from constants import *


db_config = {
    "database": "test_db",
    "user": "admin",
    "password": "root",
    "host": "localhost",
    "port": "5432",
}


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
                VendorID INTEGER, 
                tpep_pickup_datetime TIMESTAMP NOT NULL, 
                tpep_dropoff_datetime TIMESTAMP NOT NULL,
                PASSENGER_COUNT FLOAT, 
                TRIP_DISTANCE FLOAT, 
                RATECODEID FLOAT,
                store_and_fwd_flag TEXT,
                PULOCATIONID INTEGER, 
                DOLOCATIONID INTEGER, 
                PAYMENT_TYPE INTEGER,
                FARE_AMOUNT FLOAT, 
                EXTRA FLOAT, 
                MTA_TAX FLOAT, 
                TIP_AMOUNT FLOAT, 
                TOLLS_AMOUNT FLOAT, 
                IMPROVEMENT_SURCHARGE FLOAT, 
                TOTAL_AMOUNT FLOAT, 
                CONGESTION_SURCHARGE FLOAT,
                airport_fee TEXT,
                formatted_pickup_date TEXT,
                hour INTEGER,
                year_month TEXT,
                processed_time TIMESTAMP NOT NULL 
            );

            CREATE TABLE IF NOT EXISTS {} (
                year_month TEXT,
                PULocationID TEXT,
                DOLocationID TEXT,
                trip_count_array INTEGER[][],
                avg_amount_array INTEGER[][]
            );

            CREATE TABLE IF NOT EXISTS your_table (
                id INTEGER[],
                array_column INTEGER[][]
            )

        """
        ).format(
            sql.Identifier(TRIPS_TABLE_NAME),
            sql.Identifier(AGGREGATED_TRIPS_TABLE_NAME),
        )
        cur.execute(command)

        logger.info(f"Initializing with Jan 2020 data")
        run_etl(datetime.datetime(2020, 1, 1), cur)
    logger.info("Ending Initialization")

    conn.commit()
    cur.close()
    conn.close()


def is_data_available(cur, formatted_year_month):
    cur.execute(
        sql.SQL(
            """
            SELECT
                %s in (SELECT DISTINCT year_month FROM {}) as is_stored
        """
        ).format(sql.Identifier(AGGREGATED_TRIPS_TABLE_NAME)),
        [formatted_year_month],
    )
    return cur.fetchall()[0][0]


def has_exceeded_database_limit(cur):
    cur.execute(
        sql.SQL(
            """
                SELECT
                    COUNT(DISTINCT year_month)
                FROM {}
            """
        ).format(sql.Identifier(TRIPS_TABLE_NAME))
    )
    number_of_months = cur.fetchall()[0][0]
    logger.info(f"Number of year_month: {number_of_months}")
    return number_of_months == DATABASE_LIMIT


def query_aggregated_trips_data(input_date):
    logger.info(f"Querying data with input: {input_date}")
    formatted_year_month = input_date.strftime(YEAR_MONTH_FORMAT)
    # Connect with postgressql
    conn = build_connection()
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # Validate data availability & database limit
    if not is_data_available(cur, formatted_year_month):
        logger.info("Data is not available")
        if has_exceeded_database_limit(cur):
            logger.info("Database will exceed limit")
            delete_data(cur)
        run_etl(input_date, cur)

    # Query data
    logger.info(f"Querying data")
    cur.execute(
        sql.SQL(
            """
                SELECT
                    PULocationID,
                    DOLocationID,
                    trip_count_array,
                    avg_amount_array
                FROM {}
                WHERE year_month = %s
            """
        ).format(sql.Identifier(AGGREGATED_TRIPS_TABLE_NAME)),
        [formatted_year_month],
    )
    result = cur.fetchall()
    # logger.info(f"Queried result: {result[0] if len(result) > 0 else 'None'}")

    conn.commit()

    cur.close()
    conn.close()
    logger.info("Closed PostgreSQL Connection")
    logger.info(f"Ending Query")
    return {"data": result, "columns": [i[0] for i in cur.description]}


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


def import_data(df, cur, target_table):
    from io import StringIO

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t")
    buffer.seek(0)
    cur.copy_from(buffer, target_table, sep="\t")


def build_long_array_metrics(row, metric_col):
    monthly_arr = []
    metrics_length = len(row[metric_col])
    i = 0
    for day in range(1, 32):
        day_arr = []
        if day in row["day"]:
            for hour in range(24):
                trip_count = 0
                if i < metrics_length and row["hour"][i] == hour:
                    trip_count = row["trip_count"][i]
                    i += 1
                day_arr.append(str(trip_count))
        else:
            day_arr = ["0" for i in range(24)]
        monthly_arr.append("{" + ",".join(day_arr) + "}")
    if i != metrics_length:
        print("hailat")
    return "{" + ",".join(monthly_arr) + "}"


def run_etl(date, cur):
    start_time = time.time()
    formatted_year_month = date.strftime("%Y-%m")
    logger.info(f"Starting ETL for {formatted_year_month} data...")
    start_date = date.replace(day=1)
    end_date = start_date + relativedelta.relativedelta(months=1)

    filename = f"yellow_tripdata_{formatted_year_month}.parquet"
    dataset_url = build_url(filename)
    # Read file from website
    # TODO: remove .iloc[:5]
    df = pd.read_parquet(dataset_url, engine="pyarrow").iloc[:5].fillna(0)
    # Need to handle data validation
    if not ("PULocationID" in df.columns and "DOLocationID" in df.columns):
        raise Exception("No location data")

    df = df[
        (df[PICKUP_DATETIME_COL] >= start_date) & (df[PICKUP_DATETIME_COL] < end_date)
    ]
    df["formatted_pickup_date"] = df[PICKUP_DATETIME_COL].dt.strftime("%Y-%m-%d")
    df["hour"] = df[PICKUP_DATETIME_COL].dt.hour
    df["day"] = df[PICKUP_DATETIME_COL].dt.day
    df["processed_time"] = datetime.datetime.now()

    # Import fact data
    logger.info(f"Start Copying {len(df)} rows to {TRIPS_TABLE_NAME}...")
    import_data(df, cur, TRIPS_TABLE_NAME)
    logger.info("Done Copying...")

    # Import aggregated data
    agg_df = (
        df.groupby(["day", "hour", "PULocationID", "DOLocationID"]).agg(
            trip_count=(PICKUP_DATETIME_COL, "count"),
            avg_amount=(TOTAL_AMOUNT_COL, "mean"),
        )
    ).reset_index()

    grouped_agg_df = (
        agg_df.groupby(["PULocationID", "DOLocationID"])
        .agg({"day": list, "hour": list, "trip_count": list, "avg_amount": list})
        .reset_index()
    )

    raw_data = {
        "year_month": [formatted_year_month] * len(grouped_agg_df),
        "PULocationID": grouped_agg_df["PULocationID"],
        "DOLocationID": grouped_agg_df["DOLocationID"],
        "trip_count_array": grouped_agg_df.apply(
            lambda row: build_long_array_metrics(row, "trip_count"), axis=1
        ),
        "avg_amount_array": grouped_agg_df.apply(
            lambda row: build_long_array_metrics(row, "trip_count"), axis=1
        ),
    }
    aggregated_trips_df = pd.DataFrame(raw_data)

    logger.info(
        f"Start Copying {len(aggregated_trips_df)} rows to {AGGREGATED_TRIPS_TABLE_NAME}..."
    )
    import_data(
        aggregated_trips_df,
        cur,
        AGGREGATED_TRIPS_TABLE_NAME,
    )
    logger.info("Done Copying...")

    logger.info(f"Ending ETL... Time taken: {time.time() - start_time}")


# initialize()
# query_aggregated_trips_data(datetime.datetime(2020, 2, 12))


# for year in range(2015, 2022):
#     for month in range(1, 13):
#         try:
#             print(query_aggregated_trips_data(datetime.datetime(year, month, 12)))
#         except Exception as error:
#             logger.error(f"{year}-{month}")
#             logger.error(error)
