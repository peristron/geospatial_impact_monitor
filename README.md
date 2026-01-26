# 🌩️ Geospatial Impact Monitor

**A real-time geospatial intelligence tool for assessing infrastructure risks.**

The **Geospatial Impact Monitor** is a Streamlit application designed for Service Providers, Network Operations Centers (NOCs), and SaaS platforms. It accepts a list of IP addresses, geolocates them, and cross-references their locations against live weather alerts and power outage maps to determine service impact.

## 🚀 Key Features

*   **Hybrid Data Fetching:** Merges data from the **National Weather Service (NWS)** and **Iowa Environmental Mesonet (IEM)** to ensure maximum alert coverage.
*   **"Err on Caution" Engine:** If data sources conflict (e.g., one says "Moderate" and another says "Severe"), the system defaults to the higher severity to ensure no risks are missed.
*   **Point-API Fallback:** Automatically detects when NWS polygon geometries are missing (a common issue) and switches to direct coordinate-based API queries for specific IP locations.
*   **Strategic Impact Assessment:** Automatically categorizes affected clients into:
    *   📉 **Probable Offline:** Clients in active power outage zones.
    *   📈 **High Load:** Clients in severe weather zones (sheltering in place) but with power.
*   **Interactive Threat Map:** Visualizes clients, weather polygons (color-coded by type), and power outage zones on a unified map.

## 🛠️ Tech Stack

*   **Python 3.8+**
*   **Streamlit:** UI Framework.
*   **Folium:** Interactive mapping.
*   **Shapely:** Geometric analysis (polygon intersections).
*   **Pandas:** Data manipulation.

## 📦 Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/yourusername/geospatial-impact-monitor.git
    cd geospatial-impact-monitor
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

    *Ensure your `requirements.txt` includes:*
    ```text
    streamlit
    pandas
    requests
    folium
    streamlit-folium
    shapely
    openpyxl
    ```

3.  Run the application:
    ```bash
    streamlit run app.py
    ```

## 📡 Data Sources

This application aggregates public data from the following open APIs:

1.  **Geolocation:** `ip-api.com` (Batch endpoint)
2.  **Weather:** 
    *   NOAA / National Weather Service (NWS) API
    *   Iowa Environmental Mesonet (IEM) - Real-time polygon feeds
3.  **Power Outages:**
    *   HIFLD (Homeland Infrastructure Foundation-Level Data)
    *   ODIN (Oak Ridge National Laboratory)

## ⚙️ Configuration & Logic

### Severity Filtering
Users can filter alerts based on severity rank:
1.  **Minor** (Advisories)
2.  **Moderate** (Watches)
3.  **Severe** (Warnings)
4.  **Extreme** (Immediate Threat)

### The "Fallback" System
NWS alerts are often issued by "Zone" (FIPS code) rather than precise geometric shapes. This results in "Null Geometry" errors in standard mapping tools. This app solves this by:
1.  Attempting to load Polygon shapes first.
2.  If polygons are missing, it sends the specific Lat/Lon of the IP address to the NWS Point-Query API to get accurate local alerts.

## ⚠️ Disclaimer

This tool is for informational purposes and situational awareness only. It should not be used for safety-critical decision-making or as a primary source for emergency evacuations. Data availability depends on third-party public APIs which may experience downtime or latency.
