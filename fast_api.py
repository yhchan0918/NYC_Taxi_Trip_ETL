from fastapi import FastAPI
import datetime
from etl import initialize, query_aggregated_trips_data
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import pandas as pd
from constants import *


class InputData(BaseModel):
    input_date: str


app = FastAPI()

initialize()


@app.post("/trips")
def query_trips(data: InputData):
    input_date = datetime.datetime.strptime(data.input_date, YEAR_MONTH_DAY_FORMAT)
    result = query_aggregated_trips_data(input_date)

    # Return the dictionary as JSON response
    return JSONResponse(result)
