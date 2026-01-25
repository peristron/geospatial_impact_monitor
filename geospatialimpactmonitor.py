import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- DATA FETCHING FUNCTIONS ---

@st.cache_data(ttl=60) # Cache only for 60 seconds for near real-time data
def fetch_active_weather_alerts():
    """
    Fetches weather alerts from Iowa Environmental Mesonet (IEM).
    IEM aggregates NWS data instantly, avoiding government API caching/pagination issues.
    """
    # This endpoint contains ALL active US Watches/Warnings in one fast GeoJSON
    url = "https://mesonet.agron.iastate.edu/geojson/current_ww.geojson"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get('features', [])
        return []
    except Exception:
        return []

@st.cache_data(ttl=600)
def fetch_power_outages():
    """
    Fetches US County Level Power Outage data from HIFLD.
    """
    url = "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/Power_Outages_County_Level/FeatureServer/0/query"
    params = {
        'where': "Percent_Out > 0.5", 
        'outFields': "NAME,State,Percent_Out,Total_Out",
        'f': 'geojson'
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('features', [])
        return []
    except Exception:
        return []

def get_geolocation_bulk(ip_list):
    """
    Uses ip-api.com batch endpoint to geolocate IPs.
    """
    url = "http://ip-api.com/batch"
    coords = []
    
    # Clean inputs
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
        except Exception:
            pass
            
    return pd.DataFrame(coords)

# --- ANALYSIS LOGIC ---

def run_impact_analysis(df_ips, weather_features, outage_features):
    results = []
    
    weather_features = weather_features or []
    outage_features = outage_features or []

    # 1. Process Weather Polygons (IEM Format)
    weather_polys = []
    for feature in weather_features:
        geom = feature.get('geometry')
        props = feature.get('properties')
        if geom:
            try:
                # IEM uses 'phenomena' and 'significance' codes, but also provides a 'wfo' and event type
                # We map coloring based on phenomena codes if possible, or simple types
                weather_polys.append({
                    'poly': shape(geom),
                    'desc': f"{props.get('event')} (Expires: {props.get('expire', 'N/A')})",
                    'phenomena': props.get('phenomena'), # TO - Tornado, SV - Severe Tstorm, WS - Winter Storm
                    'significance': props.get('significance') # W - Warning, A - Watch, Y - Advisory
                })
            except: continue

    # 2. Process Outage Polygons
    outage_polys = []
    for feature in outage_features:
        geom = feature.get('geometry')
        props = feature.get('properties')
        if geom:
            try:
                outage_polys.append({
                    'poly': shape(geom),
                    'desc': f"Power Outage: {props.get('Percent_Out')}%"
                })
            except: continue

    # 3. Check Intersections
    for index, row in df_ips.iterrows():
        hazards = []
        is_at_risk = False
        
        if pd.notnull(row['lat']):
            point = Point(row['lon'], row['lat'])
            
            # Check Weather
            for alert in weather_polys:
                if alert['poly'].contains(point):
                    is_at_risk = True
                    hazards.append(alert['desc'])
                    # Don't break here, we want to know ALL alerts for that location (e.g. Flood AND Tornado)
            
            # Check Power
            for outage in outage_polys:
                if outage['poly'].contains(point):
                    is_at_risk = True
                    hazards.append(outage['desc'])
                    
        results.append({
            **row,
            'is_at_risk': is_at_risk,
            'risk_details': " | ".join(hazards) if hazards else "None"
        })
        
    return pd.DataFrame(results)

# --- SESSION STATE ---
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'weather_data' not in st.session_state:
    st.session_state.weather_data = None
if 'outage_data' not in st.session_state:
    st.session_state.outage_data = None

# --- UI LAYOUT ---

st.title("🌩️ Geospatial Impact Monitor")
st.markdown("""
**Sources:** Weather (Iowa Environmental Mesonet High-Speed Feed) | Power (HIFLD)
""")

with st.sidebar:
    st.header("Data Input")
    input_method = st.radio("Choose Input Method", ["Paste IP List", "Bulk Upload (CSV/Excel)"])
    
    ip_list = []
    
    if input_method == "Paste IP List":
        # Updated Default List with the Biloxi IP
        raw_input = st.text_area("Paste IPs here", "204.196.160.7\n8.8.8.8", height=150)
        if raw_input:
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
            except Exception:
                st.error("Error reading file.")

    if st.button("Run Spatial Analysis"):
        if ip_list:
            with st.spinner("Geolocating Clients..."):
                df_geo = get_geolocation_bulk(ip_list)
                
            with st.spinner("Fetching Real-Time IEM Weather Feed..."):
                weather_features = fetch_active_weather_alerts()
                
            with st.spinner("Fetching Grid Status..."):
                outage_features = fetch_power_outages()
                
            with st.spinner("Analyzing Exposure..."):
                df_final = run_impact_analysis(df_geo, weather_features, outage_features)
            
            st.session_state.analysis_results = df_final
            st.session_state.weather_data = weather_features
            st.session_state.outage_data = outage_features
        else:
            st.warning("Enter IP addresses first.")

# --- RESULTS DISPLAY ---

if st.session_state.analysis_results is not None:
    
    df_final = st.session_state.analysis_results
    weather_features = st.session_state.weather_data or []
    outage_features = st.session_state.outage_data or []

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Clients", len(df_final))
    col2.metric("Clients at Risk", len(df_final[df_final['is_at_risk'] == True]), delta_color="inverse")
    col3.metric("Active Weather Polygons", len(weather_features))

    # Map Visualization
    st.subheader("Interactive Threat Map")
    
    if not df_final.empty and pd.notnull(df_final.iloc[0]['lat']):
        center_lat = df_final['lat'].mean()
        center_lon = df_final['lon'].mean()
    else:
        center_lat, center_lon = 39.8283, -98.5795 

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # 1. Add Power Outages (Dark Grey)
    if outage_features:
        for feature in outage_features:
            style_function = lambda x: {'fillColor': '#2b2b2b', 'color': '#000000', 'fillOpacity': 0.4, 'weight': 1}
            folium.GeoJson(feature, style_function=style_function, tooltip="Power Outage").add_to(m)

    # 2. Add Weather (Colored by IEM Phenomena Code)
    if weather_features:
        for feature in weather_features:
            props = feature.get('properties', {})
            # IEM Color Logic
            phenom = props.get('phenomena', '')
            sig = props.get('significance', '')
            
            # Default
            color = '#5e5e5e' 
            opacity = 0.3
            
            # Logic: Red for Tornado/Severe, Blue for Winter, Green for Flood
            if phenom == 'TO' or phenom == 'SV': # Tornado / Severe Thunderstorm
                color = '#ff0000'
                opacity = 0.6
            elif phenom == 'FF' or phenom == 'FL': # Flash Flood / Flood
                color = '#008000'
                opacity = 0.5
            elif phenom in ['WS', 'WW', 'BZ', 'IS', 'ZR']: # Winter Storm, Winter Weather, Blizzard, Ice, Freezing Rain
                color = '#0000ff'
                opacity = 0.4
            
            style_function = lambda x, c=color, o=opacity: {'fillColor': c, 'color': c, 'fillOpacity': o, 'weight': 1}
            
            # Only add the layer if it's somewhat significant (Warning/Watch/Advisory)
            # This filters out minor 'Statement' polygons that clutter the map
            if sig in ['W', 'A', 'Y']: 
                folium.GeoJson(feature, style_function=style_function).add_to(m)

    # 3. Add IP Markers
    for _, row in df_final.iterrows():
        if pd.notnull(row['lat']):
            color = 'red' if row['is_at_risk'] else 'green'
            icon = 'exclamation-triangle' if row['is_at_risk'] else 'user'
            
            # Truncate details if they are too long for the popup
            details = row['risk_details']
            if len(details) > 100: details = details[:100] + "..."
            
            popup_html = f"""<b>IP:</b> {row['ip']}<br><b>Loc:</b> {row['city']}<br><b>Risk:</b> {details}"""
            
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon=icon, prefix='fa')
            ).add_to(m)

    st_folium(m, width=1200, height=500)
    st.dataframe(df_final)
