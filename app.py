from flask import Flask, jsonify
import requests
import pandas as pd
from datetime import datetime as dt

app = Flask(__name__)


def preprocess_data(data):
    df = pd.DataFrame(data)
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce")
    df["distance"].fillna(0, inplace=True)
    df["duration_in_seconds"] = (
        pd.to_timedelta(df["duration"]).dt.total_seconds().astype("int64")
    )
    # Calculate revenue per mile and revenue per minute
    df["revenue_per_mile"] = df["earnings"] / df["distance"]
    df["revenue_per_minute"] = df["earnings"] / df["duration_in_seconds"] * 60
    return df


def analyze_top_revenue_per_mile(dataframe, top_n=5):
    return dataframe.nlargest(top_n, "revenue_per_mile")


def analyze_top_revenue_per_minute(dataframe, top_n=5):
    return dataframe.nlargest(top_n, "revenue_per_minute")


def analyze_most_revenue_by_time(dataframe):
    # Extract day of week and hour from date_requested and time_requested columns
    dataframe["date_requested"] = pd.to_datetime(dataframe["date_requested"])
    dataframe["day_of_week"] = dataframe["date_requested"].dt.dayofweek
    dataframe["hour"] = pd.to_datetime(dataframe["time_requested"]).dt.hour

    # Calculate mean earnings by day of week and by hour
    # Day of the week should take into account that each day my have more or less hours worked
    earnings_by_day = (
        dataframe.groupby("day_of_week")["earnings"].sum()
        / dataframe.groupby("day_of_week")["date_requested"].nunique()
    )
    earnings_by_hour = (
        dataframe.groupby("hour")["earnings"].sum()
        / dataframe.groupby("hour")["date_requested"].nunique()
    )

    return earnings_by_day, earnings_by_hour


@app.route("/analyze-data")
def analyze_data():
    # Fetch data from the existing endpoint
    endpoint = "http://localhost:5000/earnings"
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()

        # Preprocess the data
        dataframe = preprocess_data(data)

        # Analyze the data
        top_revenue_per_mile = analyze_top_revenue_per_mile(dataframe)
        top_revenue_per_minute = analyze_top_revenue_per_minute(dataframe)
        earnings_by_day, earnings_by_hour = analyze_most_revenue_by_time(dataframe)

        # Convert analyzed data back to JSON
        top_revenue_per_mile_json = top_revenue_per_mile.to_dict(orient="records")
        top_revenue_per_minute_json = top_revenue_per_minute.to_dict(orient="records")
        earnings_by_day_json = earnings_by_day.to_dict()
        earnings_by_hour_json = earnings_by_hour.to_dict()

        # Return analyzed JSON data
        return jsonify(
            {
                "top_revenue_per_mile": top_revenue_per_mile_json,
                "top_revenue_per_minute": top_revenue_per_minute_json,
                "earnings_by_day": earnings_by_day_json,
                "earnings_by_hour": earnings_by_hour_json,
            }
        )
    else:
        return {"error": "Failed to fetch data"}, 400


if __name__ == "__main__":
    app.run(debug=True, port=5001)
