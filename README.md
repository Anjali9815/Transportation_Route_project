# 🚛 Transportation Route Optimization Project

This project is focused on generating, analyzing, and optimizing real-time transportation route data between major U.S. cities using a combination of **public APIs**, **big data processing with Hadoop and PySpark**, and **route optimization algorithms** such as **Greedy Nearest Neighbor**, **Simulated Annealing**, and **Google OR-Tools**.

---

## 📂 Project Structure

### 1. `data_generation_api.py`
This script is responsible for generating realistic transportation route data between cities using **6 real-time APIs**:
- Google Maps API
- OpenRouteService (ORS)
- HERE Maps API
- GraphHopper API
- Mapbox API
- OpenWeatherMap API

It gathers travel distances, durations, and weather conditions between all city-pair combinations. The enriched data is stored as a large dataset (up to 15GB) in **Hadoop Distributed File System (HDFS)** using `WebHDFS`.

---

### 2. `eda_and_analysis.py` (PySpark-based Analysis)
This module performs data fetching, cleaning, and exploratory data analysis (EDA) using **PySpark** and `pandas`.

#### 🔄 Data Ingestion
- Reads large-scale route data from HDFS using `hdfs.InsecureClient`.
- Loads into a Pandas DataFrame in chunks and then converts to a PySpark DataFrame for scalable processing.

#### 🧼 Data Cleaning
- Drops null or invalid records.
- Casts types and filters unreasonable travel durations/distances.
- Extracts new features like average speed (mph).

#### 📊 Exploratory Data Analysis (EDA)
- Summary statistics on distance, duration, temperature, and speed.
- Distribution by origin and destination cities.
- Impact of weather and temperature buckets on trip durations.
- Visualizations:
  - Seaborn scatter plots and heatmaps
  - Distance matrix pivot tables

#### 📍 Route Clustering
- Clustering cities based on route features using **K-Means** on Spark MLlib.
- Feature engineering includes string indexing and vector assembly.

---

### 3. `route_optimization.py`
Implements and compares **three TSP-based optimization algorithms** to minimize total route distance across cities.

#### 🧠 Algorithms Used
- **Greedy Nearest Neighbor**
- **Simulated Annealing**
- **Google OR-Tools TSP Solver**

Each algorithm produces:
- Optimized route (sequence of cities)
- Total travel distance

#### 📈 Output
- Tabular comparison of routes and total distances
- Route maps using **Folium** for all algorithms
- HTML map visualizations:
  - `nearest_greedy.html`
  - `simulated_annealing.html`
  - `ORtools_TSP.html`

---

## 💾 Technologies & Tools

- **Python 3.9+**
- **Hadoop (HDFS via WebHDFS)**
- **PySpark**
- **Pandas / NumPy / Seaborn / Matplotlib**
- **Folium (Mapping & Geo-visualization)**
- **Google OR-Tools (Route Solver)**
- **API Integrations**:
  - Google Maps, ORS, HERE Maps, GraphHopper, Mapbox, OpenWeatherMap

---

## 🔐 API Key Management

All API keys are securely stored in a `.env` file and loaded using `python-dotenv`. **Never hardcode or push keys to GitHub**.

```bash
# .env file example
GOOGLE_API_KEY=your_google_key
ORS_API_KEY=your_ors_key
HERE_API_KEY=your_here_key
MAPBOX_API_KEY=your_mapbox_key
GRAPHOPPER_API_KEY=your_graphhopper_key
OWM_API_KEY=your_openweathermap_key
