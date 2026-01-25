import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor - IP Risk Overlay", layout="wide")

# --- FUNCTIONS ---

@st.cache_data(ttl=300) # Cache weather data for 5 minutes
def fetch_active_weather_alerts():
    """
    Fetches active severe weather alerts from the US National Weather Service (NOAA).
    Returns a list of GeoJSON features.
    """
    url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert&severity=Severe,Extreme"
    headers = {"User-Agent": "(my-weather-app, contact@example.com)"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get('features', [])
        else:
            st.error(f"Error fetching weather data: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Connection error: {e}")
        return []

def get_geolocation_bulk(ip_list):
    """
    Uses ip-api.com batch endpoint to geolocate IPs.
    Limit: 100 IPs per batch. Free tier limitations apply.
    """
    # IP-API batch endpoint accepts max 100 IPs
    url = "http://ip-api.com/batch"
    coords = []
    
    # Chunk list into 100s
    chunk_size = 100
    for i in range(0, len(ip_list), chunk_size):
        chunk = ip_list[i:i + chunk_size]
        try:
            response = requests.post(url, json=chunk).json()
            for res in response:
                if res['status'] == 'success':
                    coords.append({
                        'ip': res['query'],
                        'lat': res['lat'],
                        'lon': res['lon'],
                        'city': res['city'],
                        'region': res['region']
                    })
                else:
                    coords.append({
                        'ip': res['query'],
                        'lat': None,
                        'lon': None,
                        'city': "N/A", 
                        'region': "N/A"
                    })
            # Respect rate limits (though batch is efficient, a small sleep is polite)
            time.sleep(1) 
        except Exception as e:
            st.error(f"Error locating IPs: {e}")
            
    return pd.DataFrame(coords)

def check_intersection(df_ips, weather_features):
    """
    Performs Point-in-Polygon analysis.
    """
    results = []
    
    # 1. Convert Weather Features to Shapely Polygons
    alert_polygons = []
    for feature in weather_features:
        geom = feature.get('geometry')
        props = feature.get('properties')
        if geom:
            try:
                poly = shape(geom)
                alert_polygons.append({
                    'poly': poly,
                    'event': props.get('event'),
                    'severity': props.get('severity'),
                    'headline': props.get('headline')
                })
            except:
                continue

    # 2. Iterate through IPs
    for index, row in df_ips.iterrows():
        is_at_risk = False
        risk_details = "None"
        
        if pd.notnull(row['lat']):
            point = Point(row['lon'], row['lat']) # Shapely uses (Lon, Lat)
            
            for alert in alert_polygons:
                if alert['poly'].contains(point):
                    is_at_risk = True
                    risk_details = f"{alert['severity']} - {alert['event']}"
                    break # Stop checking after first hit for simplicity
        
        results.append({
            **row,
            'is_at_risk': is_at_risk,
            'risk_details': risk_details
        })
        
    return pd.DataFrame(results)

# --- UI LAYOUT ---

st.title("🌩️ Geospatial Impact Monitor")
st.markdown("""
This tool performs a **point-in-polygon analysis** to determine if client IP addresses 
are located within active US National Weather Service severe weather zones.
""")

# Sidebar Input
with st.sidebar:
    st.header("Data Input")
    input_method = st.radio("Choose Input Method", ["Single IP", "Bulk Upload (CSV/Excel)"])
    
    ip_list = []
    
    if input_method == "Single IP":
        single_ip = st.text_input("Enter IP Address", "8.8.8.8")
        if single_ip:
            ip_list = [single_ip.strip()]
            
    else:
        uploaded_file = st.file_uploader("Upload file", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                # Assume the column is named 'ip' or take the first column
                col_name = 'ip' if 'ip' in df.columns.str.lower() else df.columns[0]
                ip_list = df[col_name].astype(str).tolist()
                st.success(f"Loaded {len(ip_list)} IPs")
            except Exception as e:
                st.error("Error reading file. Ensure there is a column of IPs.")

    run_analysis = st.button("Run Spatial Analysis")

# Main Execution
if run_analysis and ip_list:
    with st.spinner("Fetching Geolocation Data..."):
        # 1. Geolocate IPs
        df_geo = get_geolocation_bulk(ip_list)
        
    with st.spinner("Fetching Live Weather Polygons..."):
        # 2. Get Weather
        weather_features = fetch_active_weather_alerts()
        st.info(f"Analyzed against {len(weather_features)} active severe weather alerts.")

    with st.spinner("Calculating Spatial Intersections..."):
        # 3. Perform Overlay
        df_final = check_intersection(df_geo, weather_features)

    # --- RESULTS DISPLAY ---
    
    # Summary Metrics
    col1, col2 = st.columns(2)
    total_ips = len(df_final)
    at_risk_ips = len(df_final[df_final['is_at_risk'] == True])
    
    col1.metric("Total Clients Mapped", total_ips)
    col2.metric("Clients in Hazard Zones", at_risk_ips, delta_color="inverse")

    # Map Visualization
    st.subheader("Interactive Threat Map")
    
    # Center map on the US or the average of IPs
    if not df_final.empty and pd.notnull(df_final.iloc[0]['lat']):
        center_lat = df_final['lat'].mean()
        center_lon = df_final['lon'].mean()
    else:
        center_lat, center_lon = 39.8283, -98.5795 # US Center

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # Add Weather Polygons (Red)
    for feature in weather_features:
        style_function = lambda x: {'fillColor': '#ff0000', 'color': '#ff0000', 'fillOpacity': 0.3, 'weight': 1}
        folium.GeoJson(feature, style_function=style_function).add_to(m)

    # Add IP Markers
    for _, row in df_final.iterrows():
        if pd.notnull(row['lat']):
            color = 'red' if row['is_at_risk'] else 'green'
            icon = 'exclamation-triangle' if row['is_at_risk'] else 'user'
            
            popup_html = f"""
            <b>IP:</b> {row['ip']}<br>
            <b>Loc:</b> {row['city']}, {row['region']}<br>
            <b>Status:</b> {row['risk_details']}
            """
            
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon=icon, prefix='fa')
            ).add_to(m)

    st_folium(m, width=1200, height=500)

    # Data Table
    st.subheader("Detailed Exposure Report")
    st.dataframe(df_final)

elif run_analysis and not ip_list:
    st.warning("Please enter an IP or upload a file.")
