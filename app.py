import streamlit as st
from etl import initialize, query_aggregated_trips_data
import datetime
import requests
import pandas as pd
from constants import *


# State management

date = st.date_input("Pick a date", value=None)
if st.button("Submit Request"):
    if not date:
        st.error("Please pick a date first")
    elif date:

        year_month_date = date.strftime(YEAR_MONTH_FORMAT)
        if year_month_date in st.session_state:
            print("Retrieve from cache")
            df = st.session_state[year_month_date]
        else:

            result = requests.post(
                url="http://127.0.0.1:8000/trips",
                json={"input_date": date.strftime(YEAR_MONTH_DAY_FORMAT)},
            )
            if result.status_code != 200:
                st.error("Server Error")
            else:
                json_data = result.json()
                df = pd.DataFrame(json_data["data"], columns=json_data["columns"])
                st.session_state[year_month_date] = df

        st.dataframe(df)
