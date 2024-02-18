import streamlit as st
import requests
import pandas as pd
from constants import *


# Utility function
@st.cache_resource
def get_zone_lookup_table():
    zone_df = pd.read_csv("taxi+_zone_lookup.csv")
    duplicated_zone_id_list = [103, 104, 105, 56, 57]

    lookup = {"None": "None"}
    reverse_lookup = {"None": "None"}
    for _, row in zone_df.iterrows():
        zone_name = row["Zone"]
        location_id = row["LocationID"]
        if zone_name and location_id not in UNKNOWN_LOCATION_IDS:
            zone_name = (
                f"{zone_name} ({str(location_id)})"
                if location_id in duplicated_zone_id_list
                else zone_name
            )
            lookup[location_id] = zone_name
            reverse_lookup[zone_name] = location_id

    return lookup, reverse_lookup


# Initialization global resources and state
id2zone_lookup, zone2id_lookup = get_zone_lookup_table()


def initialize_state():
    for key in [SUBMIT_REQUEST_KEY]:
        if key not in st.session_state:
            st.session_state[key] = False


def on_change_input_date():
    st.session_state[SUBMIT_REQUEST_KEY] = False


def on_change_pickup_zone():
    pass


def on_change_dropoff_zone():
    pass


def click_submit_request_button():
    st.session_state[SUBMIT_REQUEST_KEY] = True


# App code
initialize_state()
st.title("TLC Yellow Taxi Trips Analysis")
date = st.date_input(
    "Pick a date",
    value=None,
    on_change=on_change_input_date,
    min_value=MIN_DATE,
    max_value=MAX_DATE,
)
st.button("Submit Request", on_click=click_submit_request_button)


def skeleton1():
    col1, col2 = st.columns(2)
    return col1, col2


col1, col2 = skeleton1()
location_info = st.container()


if st.session_state[SUBMIT_REQUEST_KEY]:
    if not date:
        st.error("Please pick a date first")
    elif date:
        formatted_year_month = date.strftime(YEAR_MONTH_FORMAT)
        day_index = date.day - 1
        hours_arr = [i for i in range(NO_OF_HOURS)]
        formatted_date = date.strftime(YEAR_MONTH_DAY_WITH_SLASH_FORMAT)
        if formatted_year_month in st.session_state:
            print("Retrieved from cache")
            df = st.session_state[formatted_year_month]
        else:

            result = requests.post(
                url=API_URI,
                json={"input_date": date.strftime(YEAR_MONTH_DAY_FORMAT)},
            )
            if result.status_code != 200:
                st.error("Server Error")
            else:
                json_data = result.json()
                df = pd.DataFrame(json_data["data"], columns=json_data["columns"])
                st.session_state[formatted_year_month] = df

        zone_pairs = {}
        for _, row in df.iterrows():
            daily_trip_count_metrics = row["trip_count_array"][day_index]
            pulocationid = row[PULOCATION_ID_COL]
            dolocationid = row[DOLOCATION_ID_COL]
            for idx in hours_arr:
                if (
                    daily_trip_count_metrics[idx] > 0
                    and pulocationid not in UNKNOWN_LOCATION_IDS
                    and dolocationid not in UNKNOWN_LOCATION_IDS
                ):
                    if pulocationid not in zone_pairs:
                        temp = set()
                        temp.add(dolocationid)
                        zone_pairs[pulocationid] = temp
                    else:
                        zone_pairs[pulocationid].add(dolocationid)
        with col1:
            selected_puzone_txt = st.selectbox(
                label="Pickup Zone",
                options=["None"] + list(zone_pairs.keys()),
                placeholder="Select Pickup Zone.",
                on_change=on_change_pickup_zone,
                format_func=lambda x: id2zone_lookup[x],
            )
        location_info.warning(
            "You MUST select a value each for Pickup Zone & Dropoff Zone to apply location filter."
        )
        if selected_puzone_txt != "None":
            with col2:
                selected_dozone_txt = st.selectbox(
                    label="Dropoff Zone",
                    options=["None"] + list(zone_pairs[selected_puzone_txt]),
                    placeholder="Select Dropoff Zone.",
                    on_change=on_change_dropoff_zone,
                    format_func=lambda x: id2zone_lookup[x],
                )

        # Data Aggregation
        if selected_puzone_txt != "None" and selected_dozone_txt != "None":
            selected_puzone_id = selected_puzone_txt
            selected_dozone_id = selected_dozone_txt
            print(
                "selected_puzone_id",
                selected_puzone_id,
                "selected_dozone_id",
                selected_dozone_id,
            )
            df = df[
                (df[PULOCATION_ID_COL] == selected_puzone_id)
                & (df[DOLOCATION_ID_COL] == selected_dozone_id)
            ]

        summed_trip_count = [0 for _ in range(NO_OF_HOURS)]
        summed_avg_amount = [0 for _ in range(NO_OF_HOURS)]
        for _, row in df.iterrows():
            daily_trip_count_metrics = row["trip_count_array"][day_index]
            daily_avg_amount_metrics = row["avg_amount_array"][day_index]

            for idx in hours_arr:
                summed_trip_count[idx] += daily_trip_count_metrics[idx]
                summed_avg_amount[idx] += daily_avg_amount_metrics[idx]

        if max(summed_trip_count) > 0:  # Trips that fulfill criteria are available
            trip_count_by_hour_data = pd.DataFrame(
                {"Hour": hours_arr, "Trip Count": summed_trip_count}
            )
            # Shows the trip count for each hour of that day
            st.subheader(f"Trip Count By Hour on {formatted_date}")
            st.line_chart(
                trip_count_by_hour_data,
                x="Hour",
                y="Trip Count",
            )

            # Shows average total fare for each hour of that day and the cheapest hour of the day on average to take a trip
            st.subheader(f"Average Total Fare By Hour on {formatted_date}")
            avg_amount_by_hour_data = pd.DataFrame(
                {
                    "Hour": hours_arr,
                    "Average Total Fare ($)": [
                        round(amount, 2) for amount in summed_avg_amount
                    ],
                }
            )
            st.line_chart(avg_amount_by_hour_data, x="Hour", y="Average Total Fare ($)")
            valid_hours = [hour for hour in hours_arr if summed_trip_count[hour] > 0]
            valid_avg_amount = []
            for valid_hour in valid_hours:
                valid_avg_amount.append(summed_avg_amount[valid_hour])
            min_avg_amount = min(valid_avg_amount)
            cheapest_hour_arr = []
            for hour in hours_arr:
                if summed_avg_amount[hour] == min_avg_amount and hour in valid_hours:
                    cheapest_hour_arr.append(str(hour))

            formatted_answer = (
                ", ".join(cheapest_hour_arr)
                if len(cheapest_hour_arr) > 1
                else (cheapest_hour_arr[0])
            )
            st.markdown(
                f"""Cheapest hour on average to take a trip on {formatted_date} is :green[{formatted_answer}]"""
            )
        else:  # No trips are available
            st.error(
                f"No historical trips from {selected_puzone_txt} to {selected_dozone_txt} on {formatted_date}"
            )
