from flask import Flask, jsonify
import requests
import pandas as pd
from datetime import datetime as dt

app = Flask(__name__)


def preprocess_data(data):
    df = pd.DataFrame(data)
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
    df["duration_in_seconds"] = (
        pd.to_timedelta(df["duration"]).dt.total_seconds().astype("int64")
    )
    df["revenue_per_mile"] = df["earnings"] / df["distance"]
    df["revenue_per_minute"] = df["earnings"] / df["duration_in_seconds"] * 60
    return df


def analyze_top_revenue_per_mile(dataframe, top_n=5):
    dataframe = dataframe[dataframe["distance"] > 0]
    return dataframe.nlargest(top_n, "revenue_per_mile")


def analyze_top_revenue_per_minute(dataframe, top_n=5):
    dataframe = dataframe[dataframe["duration_in_seconds"] > 0]
    return dataframe.nlargest(top_n, "revenue_per_minute")


def analyze_most_revenue_by_time(dataframe):
    dataframe["date_requested"] = pd.to_datetime(dataframe["date_requested"])
    dataframe["day_of_week"] = dataframe["date_requested"].dt.dayofweek
    dataframe["hour"] = pd.to_datetime(dataframe["time_requested"]).dt.hour
    earnings_by_day = (
        dataframe.groupby("day_of_week")["earnings"].sum()
        / dataframe.groupby("day_of_week")["date_requested"].nunique()
    )
    earnings_by_hour = (
        dataframe.groupby("hour")["earnings"].sum()
        / dataframe.groupby("hour")["date_requested"].nunique()
    )
    earnings_by_hour_for_each_day = (
        dataframe.groupby(["day_of_week", "hour"])["earnings"].sum()
        / dataframe.groupby(["day_of_week", "hour"])["date_requested"].nunique()
    )

    # Convert MultiIndex to single index with day and hour concatenated
    earnings_by_hour_for_each_day.index = [
        f"{day}-{hour}" for day, hour in earnings_by_hour_for_each_day.index
    ]

    return earnings_by_day, earnings_by_hour, earnings_by_hour_for_each_day


@app.route("/analyze-data")
def analyze_data():
    endpoint = "http://localhost:5000/earnings"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        dataframe = preprocess_data(data)
        top_revenue_per_mile = analyze_top_revenue_per_mile(dataframe)
        top_revenue_per_minute = analyze_top_revenue_per_minute(dataframe)
        (
            earnings_by_day,
            earnings_by_hour,
            earnings_by_hour_for_each_day,
        ) = analyze_most_revenue_by_time(dataframe)

        top_revenue_per_mile_json = top_revenue_per_mile.to_dict(orient="records")
        top_revenue_per_minute_json = top_revenue_per_minute.to_dict(orient="records")
        earnings_by_day_json = earnings_by_day.to_dict()
        earnings_by_hour_json = earnings_by_hour.to_dict()
        earnings_by_hour_for_each_day_json = earnings_by_hour_for_each_day.to_dict()

        return jsonify(
            {
                "top_revenue_per_mile": top_revenue_per_mile_json,
                "top_revenue_per_minute": top_revenue_per_minute_json,
                "earnings_by_day": earnings_by_day_json,
                "earnings_by_hour": earnings_by_hour_json,
                "earnings_by_hour_for_each_day": earnings_by_hour_for_each_day_json,
            }
        )
    else:
        return {"error": "Failed to fetch data"}, 400


if __name__ == "__main__":
    app.run(debug=True, port=5001)
