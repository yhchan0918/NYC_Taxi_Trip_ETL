import datetime

YEAR_MONTH_DAY_FORMAT = "%Y-%m-%d"
YEAR_MONTH_DAY_WITH_SLASH_FORMAT = "%Y/%m/%d"
YEAR_MONTH_FORMAT = "%Y-%m"
TRIPS_TABLE_NAME = "trips"
AGGREGATED_TRIPS_TABLE_NAME = "aggregate_trips"
PICKUP_DATETIME_COL = "tpep_pickup_datetime"
TOTAL_AMOUNT_COL = "total_amount"
PULOCATION_ID_COL = "pulocationid"
DOLOCATION_ID_COL = "dolocationid"
DATABASE_LIMIT = 3
NO_OF_HOURS = 24


# Streamlit
SUBMIT_REQUEST_KEY = "submit_request"
MIN_DATE = datetime.datetime(2011, 1, 1)
MAX_DATE = datetime.datetime(2023, 11, 30)
