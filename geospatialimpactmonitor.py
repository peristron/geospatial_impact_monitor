import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- FUNCTIONS ---

@st.cache_data(ttl=300)
def fetch_active_weather_alerts():
    """
    Fetches active weather alerts from NOAA.
    UPDATED: Now includes 'Moderate' severity to catch Winter Storms/Advisories.
    """
    # Added "Moderate" to the severity list
    url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert&severity=Severe,Extreme,Moderate"
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
    """
    url = "http://ip-api.com/batch"
    coords = []
    
    # Clean list: remove duplicates and empty strings
    ip_list = list(filter(None, set(ip_list)))
    
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
            time.sleep(1) 
        except Exception as e:
            st.error(f"Error locating IPs: {e}")
            
    return pd.DataFrame(coords)

def check_intersection(df_ips, weather_features):
    results = []
    
    # Pre-process polygons
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

    for index, row in df_ips.iterrows():
        is_at_risk = False
        risk_details = "None"
        
        if pd.notnull(row['lat']):
            point = Point(row['lon'], row['lat'])
            
            for alert in alert_polygons:
                if alert['poly'].contains(point):
                    is_at_risk = True
                    # Formatting the alert details for the report
                    risk_details = f"[{alert['severity']}] {alert['event']}"
                    break 
        
        results.append({
            **row,
            'is_at_risk': is_at_risk,
            'risk_details': risk_details
        })
        
    return pd.DataFrame(results)

# --- INITIALIZE SESSION STATE ---
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'weather_data' not in st.session_state:
    st.session_state.weather_data = None

# --- UI LAYOUT ---

st.title("🌩️ Geospatial Impact Monitor")

st.markdown("""
This tool leverages **public US National Weather Service data** to perform a **point-in-polygon analysis**, 
determining if client IP addresses are located within active severe weather zones.  
*Optimized for US-based IP addresses using open-source intelligence (OSINT) sources.*
""")

# Sidebar Input
with st.sidebar:
    st.header("Data Input")
    
    # NEW: Updated Input Method names
    input_method = st.radio("Choose Input Method", ["Paste IP List (Text)", "Bulk Upload (CSV/Excel)"])
    
    ip_list = []
    
    # NEW: Logic for Text Area Input
    if input_method == "Paste IP List (Text)":
        raw_input = st.text_area(
            "Paste IPs here", 
            "8.8.8.8\n1.1.1.1", 
            height=150,
            help="Enter IPs separated by commas, spaces, or new lines."
        )
        if raw_input:
            # Replaces commas with spaces, then splits by whitespace (handles newlines/spaces)
            ip_list = [ip.strip() for ip in raw_input.replace(',', ' ').split() if ip.strip()]
            
    else:
        uploaded_file = st.file_uploader("Upload file", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                col_name = 'ip' if 'ip' in df.columns.str.lower() else df.columns[0]
                ip_list = df[col_name].astype(str).tolist()
                st.success(f"Loaded {len(ip_list)} IPs")
            except Exception as e:
                st.error("Error reading file. Ensure there is a column of IPs.")

    if st.button("Run Spatial Analysis"):
        if ip_list:
            with st.spinner("Fetching Geolocation Data..."):
                df_geo = get_geolocation_bulk(ip_list)
                
            with st.spinner("Fetching Live Weather Polygons (Including Moderate Alerts)..."):
                weather_features = fetch_active_weather_alerts()
                
            with st.spinner("Calculating Spatial Intersections..."):
                df_final = check_intersection(df_geo, weather_features)
            
            st.session_state.analysis_results = df_final
            st.session_state.weather_data = weather_features
        else:
            st.warning("Please provide IP addresses first.")

# --- RESULTS DISPLAY ---

if st.session_state.analysis_results is not None:
    
    df_final = st.session_state.analysis_results
    weather_features = st.session_state.weather_data

    # Summary Metrics
    col1, col2 = st.columns(2)
    total_ips = len(df_final)
    at_risk_ips = len(df_final[df_final['is_at_risk'] == True])
    
    col1.metric("Total Clients Mapped", total_ips)
    col2.metric("Clients in Hazard Zones", at_risk_ips, delta_color="inverse")

    if weather_features:
        st.caption(f"Visualizing against {len(weather_features)} active weather cells (Severe + Moderate).")

    # Map Visualization
    st.subheader("Interactive Threat Map")
    
    if not df_final.empty and pd.notnull(df_final.iloc[0]['lat']):
        center_lat = df_final['lat'].mean()
        center_lon = df_final['lon'].mean()
    else:
        center_lat, center_lon = 39.8283, -98.5795 

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # Add Weather Polygons
    if weather_features:
        for feature in weather_features:
            props = feature.get('properties', {})
            severity = props.get('severity', 'Unknown')
            
            # Color coding based on severity
            if severity == 'Extreme':
                fill_color = '#ff0000' # Red
            elif severity == 'Severe':
                fill_color = '#ff6600' # Orange
            else: 
                fill_color = '#0000ff' # Blue (for Moderate/Winter stuff)

            style_function = lambda x, color=fill_color: {
                'fillColor': color, 
                'color': color, 
                'fillOpacity': 0.3, 
                'weight': 1
            }
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
