from flask import Flask, jsonify
import requests
import pandas as pd
from datetime import datetime as dt

app = Flask(__name__)


# create days of week dictionary and make it global
days_of_week = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}


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

    earnings_by_day.index = [days_of_week[day] for day in earnings_by_day.index]
    # Sort the earnings_by_day dataframe by the correct order of days of the week
    earnings_by_day.sort_index(
        key=lambda x: x.map({day: i for i, day in enumerate(days_of_week.values())}),
        inplace=True,
    )
    # Refactor the earnings_by_hour_for_each_day dataframe
    earnings_by_hour_for_each_day_dict = {
        days_of_week[day]: {
            str(hour): earnings_by_hour_for_each_day.loc[day, hour]
            for hour in earnings_by_hour_for_each_day.loc[day].index
        }
        for day in earnings_by_hour_for_each_day.index.levels[0]
    }
    return earnings_by_day, earnings_by_hour, earnings_by_hour_for_each_day_dict


def sum_earnings_in_four_hour_block(hourly_earnings, start_hour):
    total = 0
    for hour in range(start_hour, start_hour + 4):
        total += hourly_earnings.get(str(hour), 0)
    return total


def get_top_two_four_hour_blocks(dataframe):
    top_two_blocks = {}
    for day in days_of_week.values():
        hourly_earnings = dataframe.get(day, {})
        best_blocks = [(-1, -float("inf")), (-1, -float("inf"))]
        for hour in range(24 - 3):
            earnings = sum_earnings_in_four_hour_block(hourly_earnings, hour)
            if earnings > best_blocks[0][1]:
                if best_blocks[1][0] == -1 or abs(hour - best_blocks[1][0]) >= 4:
                    best_blocks[1] = best_blocks[0]
                    best_blocks[0] = (hour, earnings)
                elif (
                    earnings > best_blocks[1][1] and abs(hour - best_blocks[0][0]) >= 4
                ):
                    best_blocks[1] = (hour, earnings)
            elif earnings > best_blocks[1][1] and abs(hour - best_blocks[0][0]) >= 4:
                best_blocks[1] = (hour, earnings)

        top_two_blocks[day] = {
            "1st_block": {
                "start_hour": best_blocks[0][0],
                "end_hour": best_blocks[0][0] + 4,  # Round up the end_hour
                "earnings": best_blocks[0][1],
            },
            "2nd_block": {
                "start_hour": best_blocks[1][0],
                "end_hour": best_blocks[1][0] + 4,  # Round up the end_hour
                "earnings": best_blocks[1][1],
            },
        }
    return top_two_blocks


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
            earnings_by_hour_for_each_day_dict,
        ) = analyze_most_revenue_by_time(dataframe)

        top_revenue_per_mile_json = top_revenue_per_mile.to_dict(orient="records")
        top_revenue_per_minute_json = top_revenue_per_minute.to_dict(orient="records")
        earnings_by_day_json = earnings_by_day.to_dict()
        earnings_by_hour_json = earnings_by_hour.to_dict()

        earnings_by_hour_for_each_day_formatted = {}
        for day, hourly_earnings in earnings_by_hour_for_each_day_dict.items():
            day_name = days_of_week.get(day, day)  # Use 'get' method to avoid KeyError
            earnings_by_hour_for_each_day_formatted[day_name] = {
                str(hour): value for hour, value in hourly_earnings.items()
            }

        top_two_four_hour_blocks = get_top_two_four_hour_blocks(
            earnings_by_hour_for_each_day_formatted
        )

        return jsonify(
            {
                "top_revenue_per_mile": top_revenue_per_mile_json,
                "top_revenue_per_minute": top_revenue_per_minute_json,
                "earnings_by_day": earnings_by_day_json,
                "earnings_by_hour": earnings_by_hour_json,
                "earnings_by_hour_for_each_day": earnings_by_hour_for_each_day_formatted,
                "top_two_four_hour_blocks": top_two_four_hour_blocks,
            }
        )
    else:
        return {"error": "Failed to fetch data"}, 400


if __name__ == "__main__":
    app.run(debug=True, port=5001)
