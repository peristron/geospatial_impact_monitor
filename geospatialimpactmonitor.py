import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- DATA FETCHING ---

def fetch_weather_data_hybrid():
    """
    Attempts to fetch weather from IEM (Fastest). 
    Falls back to NWS (Official) if IEM fails.
    Returns: List of features
    """
    # 1. Try Iowa Environmental Mesonet (Real-time, single file)
    iem_url = "https://mesonet.agron.iastate.edu/geojson/current_ww.geojson"
    try:
        r = requests.get(iem_url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('features', []), "IEM (Real-Time Feed)"
    except:
        pass

    # 2. Fallback to NWS API (Slower, but official)
    nws_url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert"
    headers = {"User-Agent": "(my-weather-app, contact@example.com)"}
    try:
        r = requests.get(nws_url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get('features', []), "NWS API (Official Backup)"
    except:
        return [], "Connection Failed"

    return [], "No Data"

@st.cache_data(ttl=600)
def fetch_power_outages():
    """Fetches HIFLD Power Outage Data"""
    url = "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/Power_Outages_County_Level/FeatureServer/0/query"
    params = {'where': "Percent_Out > 0.5", 'outFields': "NAME,State,Percent_Out,Total_Out", 'f': 'geojson'}
    try:
        r = requests.get(url, params=params, timeout=5)
        if r.status_code == 200:
            return r.json().get('features', [])
    except:
        pass
    return []

def get_geolocation_bulk(ip_list):
    """Batched IP Geolocation"""
    url = "http://ip-api.com/batch"
    coords = []
    ip_list = list(filter(None, set(ip_list)))
    
    chunk_size = 100
    for i in range(0, len(ip_list), chunk_size):
        chunk = ip_list[i:i + chunk_size]
        try:
            response = requests.post(url, json=chunk).json()
            for res in response:
                if res['status'] == 'success':
                    coords.append({'ip': res['query'], 'lat': res['lat'], 'lon': res['lon'], 'city': res['city'], 'region': res['region']})
                else:
                    coords.append({'ip': res['query'], 'lat': None, 'lon': None, 'city': "N/A", 'region': "N/A"})
            time.sleep(0.5) 
        except:
            pass
    return pd.DataFrame(coords)

# --- ANALYSIS ---

def run_impact_analysis(df_ips, weather_features, outage_features):
    results = []
    weather_features = weather_features or []
    outage_features = outage_features or []

    # Process Weather
    weather_polys = []
    for feature in weather_features:
        geom = feature.get('geometry')
        props = feature.get('properties', {})
        if geom:
            try:
                # IEM uses 'phenomena', NWS uses 'event'. We grab both.
                event_name = props.get('event') or props.get('prod_type') or "Weather Alert"
                weather_polys.append({
                    'poly': shape(geom),
                    'desc': event_name,
                    'raw_props': props # Store raw props for coloring logic
                })
            except: continue

    # Process Outages
    outage_polys = []
    for feature in outage_features:
        geom = feature.get('geometry')
        props = feature.get('properties', {})
        if geom:
            try:
                outage_polys.append({
                    'poly': shape(geom),
                    'desc': f"Power Outage: {props.get('Percent_Out')}%"
                })
            except: continue

    # Intersections
    for index, row in df_ips.iterrows():
        hazards = []
        is_at_risk = False
        if pd.notnull(row['lat']):
            point = Point(row['lon'], row['lat'])
            
            for alert in weather_polys:
                if alert['poly'].contains(point):
                    is_at_risk = True
                    hazards.append(alert['desc'])
            
            for outage in outage_polys:
                if outage['poly'].contains(point):
                    is_at_risk = True
                    hazards.append(outage['desc'])
                    
        results.append({**row, 'is_at_risk': is_at_risk, 'risk_details': " | ".join(set(hazards)) if hazards else "None"})
        
    return pd.DataFrame(results)

# --- SESSION & UI ---

if 'analysis_results' not in st.session_state: st.session_state.analysis_results = None
if 'weather_data' not in st.session_state: st.session_state.weather_data = None
if 'weather_source' not in st.session_state: st.session_state.weather_source = "Unknown"
if 'outage_data' not in st.session_state: st.session_state.outage_data = None

st.title("🌩️ Geospatial Impact Monitor")
st.markdown("**Strategies:** Multi-Source API Fetch (IEM + NWS Backup) | No Filtering (Raw Data)")

with st.sidebar:
    st.header("Data Input")
    input_method = st.radio("Method", ["Paste IP List", "Bulk Upload"])
    ip_list = []
    
    if input_method == "Paste IP List":
        # Updated test list with Biloxi and Winter Storm areas
        raw_input = st.text_area("Paste IPs", "204.196.160.7\n129.59.1.1\n8.8.8.8", height=150)
        if raw_input: ip_list = [ip.strip() for ip in raw_input.replace(',', ' ').split() if ip.strip()]
    else:
        uploaded_file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                else: df = pd.read_excel(uploaded_file)
                col_name = 'ip' if 'ip' in df.columns.str.lower() else df.columns[0]
                ip_list = df[col_name].astype(str).tolist()
                st.success(f"Loaded {len(ip_list)} IPs")
            except: st.error("File error")

    if st.button("Run Spatial Analysis (Force Refresh)"):
        if ip_list:
            # CLEAR CACHE to ensure freshness
            fetch_active_weather_alerts = st.cache_data.clear()
            
            with st.spinner("Geolocating..."):
                df_geo = get_geolocation_bulk(ip_list)
            
            with st.spinner("Fetching Fresh Weather & Power Data..."):
                weather_features, source_name = fetch_weather_data_hybrid()
                outage_features = fetch_power_outages()
            
            with st.spinner(f"Analyzing {len(weather_features)} Weather Polygons..."):
                df_final = run_impact_analysis(df_geo, weather_features, outage_features)
            
            st.session_state.analysis_results = df_final
            st.session_state.weather_data = weather_features
            st.session_state.weather_source = source_name
            st.session_state.outage_data = outage_features
        else:
            st.warning("Input IPs required.")

# --- DISPLAY ---

if st.session_state.analysis_results is not None:
    df_final = st.session_state.analysis_results
    weather_features = st.session_state.weather_data or []
    outage_features = st.session_state.outage_data or []
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Clients", len(df_final))
    col2.metric("Clients at Risk", len(df_final[df_final['is_at_risk'] == True]), delta_color="inverse")
    col3.metric("Weather Source", st.session_state.weather_source)

    st.subheader("Interactive Threat Map")
    
    if not df_final.empty and pd.notnull(df_final.iloc[0]['lat']):
        center_lat, center_lon = df_final['lat'].mean(), df_final['lon'].mean()
    else:
        center_lat, center_lon = 39.8283, -98.5795 

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # 1. Power Outages
    for feat in outage_features:
        style = lambda x: {'fillColor': '#111111', 'color': 'black', 'fillOpacity': 0.5, 'weight': 1}
        folium.GeoJson(feat, style_function=style, tooltip="Power Outage").add_to(m)

    # 2. Weather Polygons (ROBUST COLORING)
    for feat in weather_features:
        props = feat.get('properties', {})
        # Try to find a descriptive string to color-code
        desc = str(props).lower()
        
        # Default: Grey (so we see it even if we don't recognize the code)
        color = '#808080'
        opacity = 0.3
        
        if 'tornado' in desc or 'phenomena\': \'TO' in desc: 
            color = '#ff0000' # Red
            opacity = 0.6
        elif 'thunderstorm' in desc or 'phenomena\': \'SV' in desc: 
            color = '#ffa500' # Orange
            opacity = 0.5
        elif 'flood' in desc or 'phenomena\': \'FF' in desc or 'phenomena\': \'FL' in desc: 
            color = '#008000' # Green
            opacity = 0.5
        elif 'winter' in desc or 'snow' in desc or 'ice' in desc or 'phenomena\': \'WS' in desc or 'phenomena\': \'WW' in desc:
            color = '#0000ff' # Blue
            opacity = 0.4
        elif 'marine' in desc:
            color = '#00ffff' # Cyan
        
        style = lambda x, c=color, o=opacity: {'fillColor': c, 'color': c, 'fillOpacity': o, 'weight': 1}
        folium.GeoJson(feat, style_function=style, tooltip=props.get('event', 'Alert')).add_to(m)

    # 3. IPs
    for _, row in df_final.iterrows():
        if pd.notnull(row['lat']):
            color = 'red' if row['is_at_risk'] else 'green'
            icon = 'exclamation-triangle' if row['is_at_risk'] else 'user'
            folium.Marker(
                [row['lat'], row['lon']], 
                popup=f"IP: {row['ip']}<br>{row['risk_details']}", 
                icon=folium.Icon(color=color, icon=icon, prefix='fa')
            ).add_to(m)

    st_folium(m, width=1200, height=500)
    st.dataframe(df_final)

    with st.expander("Debugger: Raw Data Inspection"):
        st.write(f"Fetched {len(weather_features)} weather polygons.")
        if weather_features:
            st.write("Sample Feature Properties:", weather_features[0].get('properties'))
