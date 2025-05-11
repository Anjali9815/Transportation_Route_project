from hdfs import InsecureClient
from google.colab import drive
import requests
import pandas as pd
import time
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()  # Load from .env

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
ORS_API_KEY = os.getenv('ORS_API_KEY')
HERE_API_KEY = os.getenv('HERE_API_KEY')
GRAPHOPPER_API_KEY = os.getenv('GRAPHOPPER_API_KEY')
MAPBOX_API_KEY = os.getenv('MAPBOX_API_KEY')
OWM_API_KEY = os.getenv('OWM_API_KEY')

# Define Cities
cities = ["Harrisburg, PA", "Nashville, TN", "Salt Lake City, UT", "Denver, CO", "New York City, NY", "Cincinnati, OH", 
          "Seattle, WA", "Orlando, FL", "Washington, DC", "Detroit, MI", "Houston, TX", "Miami, FL", "Baltimore, MD", "Pittsburgh, PA", 
          "Las Vegas, NV", "Chicago, IL", "Boston, MA", "Boise, ID", "Portland, ME", "Portland, OR"]

# API Daily Limits
api_limits = {
    "Google": 38000,
    "ORS": 18000,
    "HERE": 200000,
    "GraphHopper": 5000,
    "Mapbox": 80000,
    "OWM": 85000
}

api_counters = {key: 0 for key in api_limits.keys()}

# Utility Functions
def get_coords(city):
    url = f"https://nominatim.openstreetmap.org/search"
    params = {"q": city, "format": "json"}
    headers = {"User-Agent": "MyShippingOptimizerApp/1.0 (your_email@example.com)"}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200 and response.text.strip():
        res = response.json()
        if res:
            return float(res[0]['lon']), float(res[0]['lat'])
    else:
        print(f"Nominatim API ERROR: Status {response.status_code} | Response: {response.text}")

    time.sleep(1)  # Respect rate limits
    return None, None

def can_call_api(api_name):
    return api_counters[api_name] < api_limits[api_name]

def increment_api(api_name):
    api_counters[api_name] += 1

# API Route Call Handlers
def get_route(api_name, url, params, parse_func):
    if not can_call_api(api_name):
        return None, None
    response = requests.get(url, params=params)

    if response.status_code == 200 and response.text.strip():
        res = response.json()
        increment_api(api_name)
        return parse_func(res)
    else:
        print(f"{api_name} API ERROR: Status {response.status_code} | Response: {response.text}")
        increment_api(api_name)
        return None, None


# Parsers
def parse_google(res):
    try:
        dist = res['routes'][0]['legs'][0]['distance']['value'] / 1609.34
        dur = res['routes'][0]['legs'][0]['duration']['value'] / 3600
        return dist, dur
    except:
        return None, None

def parse_ors(res):
    try:
        dist = res["features"][0]["properties"]["segments"][0]["distance"] / 1609.34
        dur = res["features"][0]["properties"]["segments"][0]["duration"] / 3600
        return dist, dur
    except:
        return None, None

def parse_here(res):
    try:
        dist = res["routes"][0]["sections"][0]["summary"]["length"] / 1609.34
        dur = res["routes"][0]["sections"][0]["summary"]["duration"] / 3600
        return dist, dur
    except:
        return None, None

def parse_graphhopper(res):
    try:
        dist = res["paths"][0]["distance"] / 1609.34
        dur = res["paths"][0]["time"] / 3600000
        return dist, dur
    except:
        return None, None

def parse_mapbox(res):
    try:
        dist = res['routes'][0]['distance'] / 1609.34
        dur = res['routes'][0]['duration'] / 3600
        return dist, dur
    except:
        return None, None

# Weather API
def get_weather(lat, lon):
    if not can_call_api("OWM"):
        return None, None
    url = f"https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": lat, "lon": lon, "appid": OWM_API_KEY, "units": "metric"}
    res = requests.get(url, params=params).json()
    increment_api("OWM")
    try:
        return res["main"]["temp"], res["weather"][0]["main"]
    except:
        return None, None

# Generate Record
def generate_record(origin, destination, dist, dur, weather, temp, source):
    return {
        "Timestamp": datetime.now().isoformat(),
        "Origin": origin,
        "Destination": destination,
        "DistanceMiles": round(dist, 2),
        "DurationHours": round(dur, 2),
        "Weather": weather,
        "Temperature": temp,
        "SourceAPI": source
    }

# --- HDFS Upload Function ---
def upload_to_hdfs(df, hdfs_path):
    # Set up HDFS client (adjust as needed based on your Hadoop setup)
    NAMENODE_HOST = 'localhost'  # Make sure this matches your actual Hadoop setup
    WEBHDFS_PORT = '9870'
    HDFS_USER = 'anjalijha'
    webhdfs_url = f'http://{NAMENODE_HOST}:{WEBHDFS_PORT}'
    client = InsecureClient(webhdfs_url, user=HDFS_USER)

    # Write the DataFrame to HDFS
    with client.write(hdfs_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)
    print(f"Data uploaded to HDFS at {hdfs_path}")

# Prepare output
output_file = "real_time_routes.csv"
if not os.path.exists(output_file):
    pd.DataFrame(columns=["Timestamp", "Origin", "Destination", "DistanceMiles", "DurationHours", "Weather", "Temperature", "SourceAPI"]).to_csv(output_file, index=False)

# HDFS Path for storing the file
HDFS_PATH = "/user/anjalijha/real_time_routes.csv"
# Continuous Collection Loop
while True:
    records = []
    for origin in cities:
        for destination in cities:
            if origin == destination:
                continue

            orig_coords = get_coords(origin)
            dest_coords = get_coords(destination)

            if None in orig_coords + dest_coords:
                continue

            api_calls = [
                ("Google", "https://maps.googleapis.com/maps/api/directions/json", {"origin": origin, "destination": destination, "key": GOOGLE_API_KEY, "departure_time": "now"}, parse_google),
                ("ORS", "https://api.openrouteservice.org/v2/directions/driving-car", {"start": f"{orig_coords[0]},{orig_coords[1]}", "end": f"{dest_coords[0]},{dest_coords[1]}", "api_key": ORS_API_KEY}, parse_ors),
                ("HERE", "https://router.hereapi.com/v8/routes", {"transportMode": "truck", "origin": f"{orig_coords[1]},{orig_coords[0]}", "destination": f"{dest_coords[1]},{dest_coords[0]}", "return": "summary", "apikey": HERE_API_KEY}, parse_here),
                ("GraphHopper", "https://graphhopper.com/api/1/route", {"point": [f"{orig_coords[1]},{orig_coords[0]}", f"{dest_coords[1]},{dest_coords[0]}"], "vehicle": "car", "locale": "en", "key": GRAPHOPPER_API_KEY}, parse_graphhopper),
                ("Mapbox", f"https://api.mapbox.com/directions/v5/mapbox/driving/{orig_coords[0]},{orig_coords[1]};{dest_coords[0]},{dest_coords[1]}", {"access_token": MAPBOX_API_KEY, "overview": "simplified"}, parse_mapbox)
            ]

            for api_name, url, params, parser in api_calls:
                d, t = get_route(api_name, url, params, parser)
                if d:
                    weather, temp = get_weather(orig_coords[1], orig_coords[0])
                    records.append(generate_record(origin, destination, d, t, weather, temp, api_name))
                time.sleep(1)

    # Save the records to the local CSV file
    df = pd.DataFrame(records)
    df.to_csv(output_file, mode='a', header=False, index=False)
    print(f"Batch saved: {len(records)} records")

    # Upload the DataFrame to HDFS
    upload_to_hdfs(df, HDFS_PATH)

    time.sleep(300)  # Sleep 5 minutes before next batch



