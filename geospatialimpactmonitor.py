import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
from shapely.validation import make_valid
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- DATA FETCHING ---

def fetch_weather_data_hybrid():
    """
    Attempts to fetch weather from multiple sources.
    Prioritizes sources that include polygon geometry.
    Returns: (List of features, source name, debug info dict)
    """
    debug_info = {}
    
    # 1. Try Iowa Environmental Mesonet (Real-time, usually has geometry)
    iem_url = "https://mesonet.agron.iastate.edu/geojson/current_ww.geojson"
    try:
        r = requests.get(iem_url, timeout=8)
        debug_info['iem_status'] = r.status_code
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', [])
            debug_info['iem_feature_count'] = len(features)
            # Count features with actual geometry coordinates
            valid_geom_count = sum(1 for f in features 
                                   if f.get('geometry') and f['geometry'].get('coordinates'))
            debug_info['iem_valid_geom_count'] = valid_geom_count
            # Only use IEM if we got usable geometry
            if valid_geom_count > 0:
                return features, "IEM (Real-Time Feed)", debug_info
            else:
                debug_info['iem_note'] = "No valid geometries found"
    except Exception as e:
        debug_info['iem_error'] = str(e)

    # 2. Fallback to NWS API
    nws_url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert"
    headers = {"User-Agent": "(geospatial-impact-monitor, contact@example.com)"}
    try:
        r = requests.get(nws_url, headers=headers, timeout=15)
        debug_info['nws_status'] = r.status_code
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', [])
            debug_info['nws_feature_count'] = len(features)
            # Count features with actual geometry
            valid_geom_count = sum(1 for f in features 
                                   if f.get('geometry') and f['geometry'].get('coordinates'))
            debug_info['nws_valid_geom_count'] = valid_geom_count
            debug_info['nws_null_geom_count'] = len(features) - valid_geom_count
            return features, "NWS API (Official)", debug_info
    except Exception as e:
        debug_info['nws_error'] = str(e)
        return [], "Connection Failed", debug_info

    return [], "No Data", debug_info


def check_point_alerts_nws(lat, lon):
    """
    Fallback: Check NWS alerts for a specific geographic point.
    This works even when polygon geometry is null in the main feed.
    """
    url = f"https://api.weather.gov/alerts/active?point={lat},{lon}"
    headers = {"User-Agent": "(geospatial-impact-monitor, contact@example.com)"}
    try:
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', [])
            alerts = []
            for f in features:
                props = f.get('properties', {})
                event = props.get('event', 'Weather Alert')
                severity = props.get('severity', '')
                alerts.append(f"{event} ({severity})" if severity else event)
            return alerts
    except:
        pass
    return []


@st.cache_data(ttl=600)
def fetch_power_outages():
    """Fetches HIFLD Power Outage Data"""
    url = "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/Power_Outages_County_Level/FeatureServer/0/query"
    params = {
        'where': "Percent_Out > 0.5", 
        'outFields': "NAME,State,Percent_Out,Total_Out", 
        'f': 'geojson'
    }
    try:
        r = requests.get(url, params=params, timeout=8)
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
            response = requests.post(url, json=chunk, timeout=10).json()
            for res in response:
                if res.get('status') == 'success':
                    coords.append({
                        'ip': res['query'], 
                        'lat': res['lat'], 
                        'lon': res['lon'], 
                        'city': res.get('city', 'N/A'), 
                        'region': res.get('regionName', res.get('region', 'N/A'))
                    })
                else:
                    coords.append({
                        'ip': res.get('query', 'Unknown'), 
                        'lat': None, 
                        'lon': None, 
                        'city': "N/A", 
                        'region': "N/A"
                    })
            time.sleep(0.5)
        except:
            pass
    return pd.DataFrame(coords)


# --- ANALYSIS ---

def run_impact_analysis(df_ips, weather_features, outage_features, enable_point_fallback=True):
    """
    Performs spatial intersection analysis.
    Includes point-based API fallback when polygon data is unavailable.
    """
    results = []
    weather_features = weather_features or []
    outage_features = outage_features or []
    
    # Geometry processing statistics
    geom_stats = {
        'total_features': len(weather_features), 
        'valid_polygons': 0, 
        'null_geometry': 0, 
        'parse_errors': 0
    }

    # --- Build Weather Polygon List ---
    weather_polys = []
    for feature in weather_features:
        geom = feature.get('geometry')
        props = feature.get('properties', {})
        
        # Check for null/missing geometry (common with NWS zone-based alerts)
        if geom is None:
            geom_stats['null_geometry'] += 1
            continue
        
        # Check for empty coordinates
        if not geom.get('type') or not geom.get('coordinates'):
            geom_stats['null_geometry'] += 1
            continue
            
        try:
            poly = shape(geom)
            
            # Fix invalid geometries (self-intersections, etc.)
            if not poly.is_valid:
                poly = make_valid(poly)
            
            if poly.is_valid and not poly.is_empty:
                # Handle both IEM (phenomena) and NWS (event) naming
                event_name = (props.get('event') or 
                              props.get('prod_type') or 
                              props.get('phenomena', 'WX'))
                weather_polys.append({
                    'poly': poly,
                    'desc': str(event_name).strip() or "Weather Alert",
                    'severity': props.get('severity', 'Unknown'),
                    'raw_props': props
                })
                geom_stats['valid_polygons'] += 1
            else:
                geom_stats['parse_errors'] += 1
        except Exception:
            geom_stats['parse_errors'] += 1
            continue
    
    # Store stats for debugging display
    st.session_state.geom_stats = geom_stats

    # --- Build Outage Polygon List ---
    outage_polys = []
    for feature in outage_features:
        geom = feature.get('geometry')
        props = feature.get('properties', {})
        if geom and geom.get('coordinates'):
            try:
                poly = shape(geom)
                if not poly.is_valid:
                    poly = make_valid(poly)
                if poly.is_valid and not poly.is_empty:
                    outage_polys.append({
                        'poly': poly,
                        'desc': f"Power Outage: {props.get('Percent_Out', 'N/A')}% - {props.get('NAME', 'Unknown')}"
                    })
            except:
                continue

    # --- Determine if we need point-based fallback ---
    # Use fallback if most features have null geometry
    use_point_fallback = (enable_point_fallback and 
                          geom_stats['valid_polygons'] < geom_stats['total_features'] * 0.1 and
                          geom_stats['total_features'] > 0)
    
    st.session_state.using_point_fallback = use_point_fallback

    # --- Analyze Each IP Location ---
    for index, row in df_ips.iterrows():
        hazards = []
        is_at_risk = False
        check_method = "polygon"
        
        if pd.notnull(row.get('lat')) and pd.notnull(row.get('lon')):
            point = Point(row['lon'], row['lat'])
            
            # Method 1: Check against weather polygons
            for alert in weather_polys:
                try:
                    # Use both contains and small buffer intersection for edge cases
                    if alert['poly'].contains(point) or alert['poly'].intersects(point.buffer(0.001)):
                        is_at_risk = True
                        hazards.append(alert['desc'])
                except:
                    continue
            
            # Method 2: Check against outage polygons
            for outage in outage_polys:
                try:
                    if outage['poly'].contains(point):
                        is_at_risk = True
                        hazards.append(outage['desc'])
                except:
                    continue
            
            # Method 3: FALLBACK - Direct NWS point query when polygon data is insufficient
            if use_point_fallback and not is_at_risk:
                point_alerts = check_point_alerts_nws(row['lat'], row['lon'])
                if point_alerts:
                    is_at_risk = True
                    hazards.extend(point_alerts)
                    check_method = "point-api"
                # Rate limit for API calls
                time.sleep(0.2)
                    
        results.append({
            'ip': row.get('ip'),
            'lat': row.get('lat'),
            'lon': row.get('lon'),
            'city': row.get('city'),
            'region': row.get('region'),
            'is_at_risk': is_at_risk, 
            'risk_details': " | ".join(sorted(set(hazards))) if hazards else "None",
            'check_method': check_method
        })
        
    return pd.DataFrame(results)


# --- SESSION STATE ---

if 'analysis_results' not in st.session_state: 
    st.session_state.analysis_results = None
if 'weather_data' not in st.session_state: 
    st.session_state.weather_data = None
if 'weather_source' not in st.session_state: 
    st.session_state.weather_source = "Unknown"
if 'fetch_debug' not in st.session_state:
    st.session_state.fetch_debug = {}
if 'outage_data' not in st.session_state: 
    st.session_state.outage_data = None
if 'geom_stats' not in st.session_state:
    st.session_state.geom_stats = {}
if 'using_point_fallback' not in st.session_state:
    st.session_state.using_point_fallback = False


# --- UI ---

st.title("🌩️ Geospatial Impact Monitor")
st.markdown("**Strategies:** Multi-Source API (IEM + NWS) | Point-Query Fallback | Geometry Validation")

with st.sidebar:
    st.header("Data Input")
    input_method = st.radio("Method", ["Paste IP List", "Bulk Upload"])
    ip_list = []
    
    if input_method == "Paste IP List":
        # Test IPs: Mix of locations that may/may not have active weather
        raw_input = st.text_area(
            "Paste IPs (one per line or comma-separated)", 
            "204.196.160.7\n129.59.1.1\n8.8.8.8\n165.134.241.141", 
            height=150
        )
        if raw_input: 
            ip_list = [ip.strip() for ip in raw_input.replace(',', '\n').split('\n') if ip.strip()]
    else:
        uploaded_file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'): 
                    df = pd.read_csv(uploaded_file)
                else: 
                    df = pd.read_excel(uploaded_file)
                # Find IP column
                ip_col = None
                for col in df.columns:
                    if 'ip' in col.lower():
                        ip_col = col
                        break
                if ip_col is None:
                    ip_col = df.columns[0]
                ip_list = df[ip_col].astype(str).tolist()
                st.success(f"Loaded {len(ip_list)} IPs from '{ip_col}'")
            except Exception as e: 
                st.error(f"File error: {e}")

    st.divider()
    enable_fallback = st.checkbox("Enable Point-API Fallback", value=True, 
                                   help="Query NWS directly for each IP location when polygon geometry is unavailable")

    if st.button("🔄 Run Spatial Analysis", type="primary"):
        if ip_list:
            # Clear cache for fresh data
            st.cache_data.clear()
            
            with st.spinner("📍 Geolocating IPs..."):
                df_geo = get_geolocation_bulk(ip_list)
            
            with st.spinner("🌦️ Fetching Weather & Power Data..."):
                weather_features, source_name, fetch_debug = fetch_weather_data_hybrid()
                outage_features = fetch_power_outages()
            
            with st.spinner(f"🔍 Analyzing against {len(weather_features)} weather features..."):
                df_final = run_impact_analysis(df_geo, weather_features, outage_features, enable_fallback)
            
            st.session_state.analysis_results = df_final
            st.session_state.weather_data = weather_features
            st.session_state.weather_source = source_name
            st.session_state.fetch_debug = fetch_debug
            st.session_state.outage_data = outage_features
            
            st.success("Analysis complete!")
        else:
            st.warning("Please input at least one IP address.")


# --- DISPLAY RESULTS ---

if st.session_state.analysis_results is not None:
    df_final = st.session_state.analysis_results
    weather_features = st.session_state.weather_data or []
    outage_features = st.session_state.outage_data or []
    geom_stats = st.session_state.geom_stats
    
    # --- Metrics Row ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Clients", len(df_final))
    
    at_risk_count = len(df_final[df_final['is_at_risk'] == True])
    col2.metric("Clients at Risk", at_risk_count, 
                delta=f"{at_risk_count}" if at_risk_count > 0 else None,
                delta_color="inverse")
    
    col3.metric("Weather Source", st.session_state.weather_source)
    
    valid_polys = geom_stats.get('valid_polygons', 0)
    total_feats = geom_stats.get('total_features', 0)
    col4.metric("Valid Polygons", f"{valid_polys}/{total_feats}")
    
    # Warning banner if using fallback
    if st.session_state.using_point_fallback:
        st.warning("⚠️ **Point-API Fallback Active**: Most weather alerts lack polygon geometry. "
                   "Using direct NWS point queries for each IP location.")

    # --- Map ---
    st.subheader("Interactive Threat Map")
    
    if not df_final.empty and pd.notnull(df_final.iloc[0].get('lat')):
        center_lat = df_final['lat'].mean()
        center_lon = df_final['lon'].mean()
    else:
        center_lat, center_lon = 39.8283, -98.5795

    m = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles='CartoDB positron')

    # Layer: Power Outages (black)
    for feat in outage_features:
        try:
            style = lambda x: {'fillColor': '#111111', 'color': 'black', 'fillOpacity': 0.5, 'weight': 1}
            props = feat.get('properties', {})
            tooltip = f"Outage: {props.get('NAME', 'Unknown')} - {props.get('Percent_Out', '?')}%"
            folium.GeoJson(feat, style_function=style, tooltip=tooltip).add_to(m)
        except:
            continue

    # Layer: Weather Polygons (color-coded by type)
    for feat in weather_features:
        geom = feat.get('geometry')
        if not geom or not geom.get('coordinates'):
            continue  # Skip null geometry features for map display
            
        props = feat.get('properties', {})
        desc = str(props).lower()
        
        # Default styling
        color = '#808080'
        opacity = 0.3
        
        # Color by alert type
        if 'tornado' in desc or "'to'" in desc:
            color, opacity = '#FF0000', 0.7  # Red
        elif 'thunderstorm' in desc or 'severe' in desc or "'sv'" in desc:
            color, opacity = '#FFA500', 0.5  # Orange
        elif 'flood' in desc or "'ff'" in desc or "'fl'" in desc:
            color, opacity = '#228B22', 0.5  # Green
        elif 'winter' in desc or 'snow' in desc or 'ice' in desc or 'blizzard' in desc or "'ws'" in desc or "'ww'" in desc:
            color, opacity = '#1E90FF', 0.5  # Blue
        elif 'cold' in desc or 'freeze' in desc or 'frost' in desc or 'wind chill' in desc:
            color, opacity = '#00CED1', 0.4  # Cyan
        elif 'heat' in desc or 'excessive' in desc:
            color, opacity = '#FF4500', 0.5  # OrangeRed
        elif 'wind' in desc or 'gale' in desc:
            color, opacity = '#9370DB', 0.4  # Purple
        elif 'fire' in desc or 'red flag' in desc:
            color, opacity = '#DC143C', 0.6  # Crimson
        elif 'marine' in desc or 'coastal' in desc:
            color, opacity = '#00FFFF', 0.3  # Aqua
        
        try:
            style = lambda x, c=color, o=opacity: {'fillColor': c, 'color': c, 'fillOpacity': o, 'weight': 1}
            tooltip = props.get('event') or props.get('prod_type') or 'Weather Alert'
            folium.GeoJson(feat, style_function=style, tooltip=tooltip).add_to(m)
        except:
            continue

    # Layer: IP Markers
    for _, row in df_final.iterrows():
        if pd.notnull(row.get('lat')):
            is_risk = row.get('is_at_risk', False)
            color = 'red' if is_risk else 'green'
            icon = 'exclamation-triangle' if is_risk else 'check'
            
            popup_html = f"""
            <b>IP:</b> {row.get('ip', 'N/A')}<br>
            <b>Location:</b> {row.get('city', 'N/A')}, {row.get('region', 'N/A')}<br>
            <b>Status:</b> {'⚠️ AT RISK' if is_risk else '✅ Clear'}<br>
            <b>Details:</b> {row.get('risk_details', 'None')}
            """
            
            folium.Marker(
                [row['lat'], row['lon']], 
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon=icon, prefix='fa')
            ).add_to(m)

    st_folium(m, width=1200, height=500, returned_objects=[])

    # --- Data Table ---
    st.subheader("Analysis Results")
    
    # Highlight at-risk rows
    def highlight_risk(row):
        if row['is_at_risk']:
            return ['background-color: #ffcccc'] * len(row)
        return [''] * len(row)
    
    display_cols = ['ip', 'city', 'region', 'is_at_risk', 'risk_details']
    if 'check_method' in df_final.columns:
        display_cols.append('check_method')
    
    st.dataframe(
        df_final[display_cols].style.apply(highlight_risk, axis=1),
        use_container_width=True
    )

    # --- Debug Expander ---
    with st.expander("🔧 Debug Information"):
        st.subheader("Fetch Statistics")
        st.json(st.session_state.fetch_debug)
        
        st.subheader("Geometry Processing Statistics")
        st.json(st.session_state.geom_stats)
        
        if geom_stats.get('null_geometry', 0) > 0:
            null_pct = (geom_stats['null_geometry'] / geom_stats['total_features']) * 100
            st.warning(f"⚠️ {null_pct:.1f}% of weather features have NULL geometry. "
                       "This is typical for NWS zone-based alerts. Point-API fallback recommended.")
        
        st.subheader("Sample Feature (Raw)")
        if weather_features:
            sample = weather_features[0]
            st.write("**Geometry Present:**", sample.get('geometry') is not None)
            st.write("**Geometry Type:**", sample.get('geometry', {}).get('type') if sample.get('geometry') else "NULL")
            st.write("**Properties:**")
            st.json(sample.get('properties', {}))
