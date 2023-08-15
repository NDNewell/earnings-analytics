from flask import Flask
import requests

app = Flask(__name__)


@app.route("/fetch-data")
def fetch_data():
    # Replace with the actual endpoint of the existing application
    endpoint = "http://localhost:5000/earnings"
    response = requests.get(endpoint)

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Failed to fetch data"}, 400


if __name__ == "__main__":
    app.run(debug=True, port=5001)
