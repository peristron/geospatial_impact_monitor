import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
from shapely.validation import make_valid
import time
from datetime import datetime

# --- CONFIGURATION ---

st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- SEVERITY CONFIGURATION ---

SEVERITY_LEVELS = {
    'Extreme': 4,
    'Severe': 3,
    'Moderate': 2,
    'Minor': 1,
    'Unknown': 0
}

# Event types that are typically informational/low-impact
LOW_PRIORITY_EVENTS = [
    'Special Weather Statement',
    'Air Quality Alert',
    'Beach Hazards Statement',
    'Rip Current Statement',
    'Marine Weather Statement',
    'Hydrologic Outlook',
    'Short Term Forecast',
    'Hazardous Weather Outlook'
]

def get_severity_rank(severity_str):
    """Convert severity string to numeric rank for comparison."""
    if not severity_str:
        return 0
    return SEVERITY_LEVELS.get(severity_str, 0)

def passes_severity_threshold(alert_props, min_severity_rank, exclude_low_priority=False):
    """Check if an alert meets the severity threshold criteria."""
    severity = alert_props.get('severity', 'Unknown')
    event = alert_props.get('event', '')

    # Check severity rank
    if get_severity_rank(severity) < min_severity_rank:
        return False

    # Optionally exclude low-priority event types
    if exclude_low_priority and event in LOW_PRIORITY_EVENTS:
        return False

    return True

# --- DATA FETCHING ---

def fetch_weather_data_hybrid():
    """
    Fetches weather from BOTH sources and merges results.
    Errs on the side of caution by including alerts from all available sources.
    Returns: (List of features, source name, debug info dict)
    """
    debug_info = {}
    all_features = []
    sources_used = []

    # 1. Try Iowa Environmental Mesonet (Real-time, usually has geometry)
    iem_url = "https://mesonet.agron.iastate.edu/geojson/sbw.geojson"
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
            if valid_geom_count > 0:
                all_features.extend(features)
                sources_used.append("IEM")
            else:
                debug_info['iem_note'] = "No valid geometries found"
    except Exception as e:
        debug_info['iem_error'] = str(e)

    # 2. ALSO try NWS API (union, not fallback)
    nws_url = "https://api.weather.gov/alerts/active?status=actual&message_type=alert"
    headers = {"User-Agent": "(geospatial-impact-monitor, contact@example.com)"}
    try:
        r = requests.get(nws_url, headers=headers, timeout=15)
        debug_info['nws_status'] = r.status_code
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', [])
            debug_info['nws_feature_count'] = len(features)
            
            # Capture NWS source-reported update time
            nws_updated = data.get('updated')
            if nws_updated:
                debug_info['nws_updated_raw'] = nws_updated
                # Parse ISO format: "2024-01-15T14:30:00+00:00"
                try:
                    # Handle various ISO formats
                    if nws_updated.endswith('Z'):
                        nws_updated = nws_updated[:-1] + '+00:00'
                    updated_dt = datetime.fromisoformat(nws_updated.replace('Z', '+00:00'))
                    debug_info['nws_updated_parsed'] = updated_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                except:
                    debug_info['nws_updated_parsed'] = nws_updated
            
            # Count features with actual geometry
            valid_geom_count = sum(1 for f in features 
                                   if f.get('geometry') and f['geometry'].get('coordinates'))
            debug_info['nws_valid_geom_count'] = valid_geom_count
            debug_info['nws_null_geom_count'] = len(features) - valid_geom_count
            # Include ALL NWS features (even those with null geometry for point-fallback)
            all_features.extend(features)
            sources_used.append("NWS")
    except Exception as e:
        debug_info['nws_error'] = str(e)

    # Determine source name for display
    if sources_used:
        source_name = " + ".join(sources_used) + " (Merged)"
    else:
        source_name = "No Data"
    
    debug_info['sources_used'] = sources_used
    debug_info['total_merged_features'] = len(all_features)
    
    return all_features, source_name, debug_info

def check_point_alerts_nws(lat, lon, min_severity_rank=0, exclude_low_priority=False):
    """
    Fallback: Check NWS alerts for a specific geographic point.
    This works even when polygon geometry is null in the main feed.
    Returns list of dicts with alert details for filtering.
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

                # Apply severity filter
                if not passes_severity_threshold(props, min_severity_rank, exclude_low_priority):
                    continue
                
                event = props.get('event', 'Weather Alert')
                severity = props.get('severity', 'Unknown')
                urgency = props.get('urgency', '')
                
                # Format: "Tornado Warning (Extreme)" or "Winter Storm Warning (Severe/Immediate)"
                if urgency and urgency != 'Unknown':
                    alerts.append(f"{event} ({severity}/{urgency})")
                else:
                    alerts.append(f"{event} ({severity})")
            return alerts
    except:
        pass
    return []

@st.cache_data(ttl=600)
def fetch_power_outages():
    """
    Fetches power outage data from BOTH sources and merges results.
    Errs on the side of caution by including outages from all available sources.
    """
    all_features = []
    seen_counties = set()  # Track to avoid duplicates

    # 1. Try HIFLD (ArcGIS)
    hifld_url = "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/Power_Outages_County_Level/FeatureServer/0/query"
    hifld_params = {
        'where': "Percent_Out > 0.5", 
        'outFields': "NAME,State,Percent_Out,Total_Out", 
        'f': 'geojson'
    }
    try:
        r = requests.get(hifld_url, params=hifld_params, timeout=8)
        if r.status_code == 200:
            features = r.json().get('features', [])
            for feat in features:
                props = feat.get('properties', {})
                county_key = f"{props.get('NAME', '')}_{props.get('State', '')}"
                if county_key not in seen_counties:
                    all_features.append(feat)
                    seen_counties.add(county_key)
    except:
        pass

    # 2. ALSO try ODIN (ORNL/OpenDataSoft) - merge, not fallback
    odin_url = "https://ornl.opendatasoft.com/api/explore/v2.1/catalog/datasets/odin-real-time-outages-county/exports/geojson"
    odin_params = {
        'where': 'percent_out > 0.5',
        'select': 'county,state,percent_out,customers_out,geo_shape'
    }
    try:
        r = requests.get(odin_url, params=odin_params, timeout=8)
        if r.status_code == 200:
            data = r.json()
            for feat in data.get('features', []):
                props = feat['properties']
                county_key = f"{props.get('county', '')}_{props.get('state', '')}"
                
                # If already seen from HIFLD, compare and keep higher percent_out (err on caution)
                if county_key in seen_counties:
                    # Find existing and compare - take max
                    for existing in all_features:
                        ex_props = existing.get('properties', {})
                        ex_key = f"{ex_props.get('NAME', '')}_{ex_props.get('State', '')}"
                        if ex_key == county_key:
                            existing_pct = ex_props.get('Percent_Out', 0) or 0
                            new_pct = props.get('percent_out', 0) or 0
                            if new_pct > existing_pct:
                                ex_props['Percent_Out'] = new_pct
                                ex_props['Total_Out'] = props.get('customers_out', ex_props.get('Total_Out'))
                            break
                else:
                    # Normalize properties to match HIFLD format
                    props['NAME'] = props.get('county')
                    props['State'] = props.get('state')
                    props['Percent_Out'] = props.get('percent_out')
                    props['Total_Out'] = props.get('customers_out')
                    all_features.append(feat)
                    seen_counties.add(county_key)
    except:
        pass

    return all_features

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

def run_impact_analysis(df_ips, weather_features, outage_features,
                        enable_point_fallback=True,
                        min_severity_rank=0,
                        exclude_low_priority=False):
    """
    Performs spatial intersection analysis.
    Includes point-based API fallback when polygon data is unavailable.
    Filters alerts by severity threshold.
    """
    results = []
    weather_features = weather_features or []
    outage_features = outage_features or []

    # Track filter statistics
    filter_stats = {'total_alerts': 0, 'passed_filter': 0, 'filtered_out': 0}

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
        
        # Apply severity filter BEFORE geometry processing (efficiency)
        filter_stats['total_alerts'] += 1
        if not passes_severity_threshold(props, min_severity_rank, exclude_low_priority):
            filter_stats['filtered_out'] += 1
            continue
        filter_stats['passed_filter'] += 1
            
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
                severity = props.get('severity', 'Unknown')
                urgency = props.get('urgency', '')
                
                # Include severity in description for clarity
                if urgency and urgency != 'Unknown':
                    desc_full = f"{event_name} ({severity}/{urgency})"
                else:
                    desc_full = f"{event_name} ({severity})"
                
                weather_polys.append({
                    'poly': poly,
                    'desc': desc_full,
                    'severity': severity,
                    'raw_props': props
                })
                geom_stats['valid_polygons'] += 1
            else:
                geom_stats['parse_errors'] += 1
        except Exception:
            geom_stats['parse_errors'] += 1
            continue

    # Store stats in session state
    st.session_state.filter_stats = filter_stats
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
                point_alerts = check_point_alerts_nws(
                    row['lat'], row['lon'], 
                    min_severity_rank=min_severity_rank,
                    exclude_low_priority=exclude_low_priority
                )
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

# --- FRESHNESS HELPER ---

def get_freshness_info(fetch_timestamp):
    """
    Calculate data freshness metrics.
    Returns: (age_str, freshness_icon, is_stale)
    """
    if not fetch_timestamp:
        return "Unknown", "⚪", False
    
    age_seconds = (datetime.now() - fetch_timestamp).total_seconds()
    age_min = int(age_seconds // 60)
    
    if age_min < 1:
        return "just now", "🟢", False
    elif age_min < 5:
        return f"{age_min} min ago", "🟢", False
    elif age_min < 10:
        return f"{age_min} min ago", "🟢", False
    elif age_min < 15:
        return f"{age_min} min ago", "🟡", False
    elif age_min < 30:
        return f"{age_min} min ago", "🟠", True
    elif age_min < 60:
        return f"{age_min} min ago", "🔴", True
    else:
        hours = age_min // 60
        return f"{hours}h {age_min % 60}m ago", "🔴", True

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
if 'filter_stats' not in st.session_state:
    st.session_state.filter_stats = {}
if 'geo_data' not in st.session_state:
    st.session_state.geo_data = None
if 'enable_fallback' not in st.session_state:
    st.session_state.enable_fallback = True
if 'min_severity_rank' not in st.session_state:
    st.session_state.min_severity_rank = 2  # Default to Moderate+
if 'exclude_low_priority' not in st.session_state:
    st.session_state.exclude_low_priority = True
if 'fetch_timestamp' not in st.session_state:
    st.session_state.fetch_timestamp = None
if 'nws_source_updated' not in st.session_state:
    st.session_state.nws_source_updated = None

# --- UI ---

st.title("🌩️ Geospatial Impact Monitor")
st.markdown("How it works: Enter IP addresses (from clients, users, or devices) to assess risks from active weather alerts and power outages. The app geolocates each IP, then checks against ~real-time data from NOAA and other sources using zone intersections and targeted queries for accurate impact detection. The engine 'errs on the side of caution', so if 1 datasource shows a severe alert, and another related datasource shows moderate, the app shows 'severe'.")

# --- CALLBACK FUNCTION FOR FILTER CHANGES ---

def rerun_analysis_with_filters():
    """Re-run analysis with current filter settings using cached data."""
    if (st.session_state.geo_data is not None and 
        st.session_state.weather_data is not None):
        
        df_final = run_impact_analysis(
            st.session_state.geo_data,
            st.session_state.weather_data,
            st.session_state.outage_data,
            enable_point_fallback=st.session_state.enable_fallback,
            min_severity_rank=st.session_state.min_severity_rank,
            exclude_low_priority=st.session_state.exclude_low_priority
        )
        st.session_state.analysis_results = df_final

with st.sidebar:
    st.header("Data Input")
    input_method = st.radio("Method", ["Paste IP List", "Bulk Upload"])
    ip_list = []

    if input_method == "Paste IP List":
        # Default IPs: higher-ed and/or k12 educational institutions using D2L Brightspace in the US South/Southeast
        raw_input = st.text_area(
            "Paste IPs (1 per line or comma-separated)", 
            "152.97.17.168\n34.236.193.193\n24.199.66.32\n141.193.213.20\n23.185.0.2\n104.153.195.192\n128.23.35.87\n104.17.163.123", 
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
    st.subheader("⚙️ Alert Filters")

    severity_options = ['All Alerts', 'Minor+', 'Moderate+', 'Severe+', 'Extreme Only']
    
    # Map selection to numeric threshold
    severity_map = {
        'All Alerts': 0,
        'Minor+': 1,
        'Moderate+': 2,
        'Severe+': 3,
        'Extreme Only': 4
    }
    
    # Reverse map to get default value
    reverse_severity_map = {v: k for k, v in severity_map.items()}
    default_severity = reverse_severity_map.get(st.session_state.min_severity_rank, 'Moderate+')
    
    severity_choice = st.select_slider(
        "Minimum Severity",
        options=severity_options,
        value=default_severity,
        key="severity_slider",
        help="Filter out lower-severity alerts. 'Moderate+' is recommended to reduce noise."
    )
    
    # Update session state and trigger re-analysis if changed
    new_severity_rank = severity_map[severity_choice]
    if new_severity_rank != st.session_state.min_severity_rank:
        st.session_state.min_severity_rank = new_severity_rank
        rerun_analysis_with_filters()

    exclude_low_priority = st.checkbox(
        "Exclude Informational Alerts", 
        value=st.session_state.exclude_low_priority,
        key="exclude_checkbox",
        help="Filter out 'Special Weather Statement', 'Air Quality Alert', etc."
    )
    
    # Update session state and trigger re-analysis if changed
    if exclude_low_priority != st.session_state.exclude_low_priority:
        st.session_state.exclude_low_priority = exclude_low_priority
        rerun_analysis_with_filters()

    st.divider()
    enable_fallback = st.checkbox(
        "Enable Point-API Fallback", 
        value=st.session_state.enable_fallback,
        key="fallback_checkbox",
        help="Query NWS directly for each IP location when polygon geometry is unavailable"
    )
    
    # Update session state and trigger re-analysis if changed
    if enable_fallback != st.session_state.enable_fallback:
        st.session_state.enable_fallback = enable_fallback
        rerun_analysis_with_filters()

    if st.button("🔄 Run Spatial Analysis", type="primary"):
        if ip_list:
            # Clear cache for fresh data
            st.cache_data.clear()
            
            with st.spinner("📍 Geolocating IPs..."):
                df_geo = get_geolocation_bulk(ip_list)
                st.session_state.geo_data = df_geo  # Store for re-analysis
            
            with st.spinner("🌦️ Fetching Weather & Power Data (merging sources)..."):
                weather_features, source_name, fetch_debug = fetch_weather_data_hybrid()
                outage_features = fetch_power_outages()
                
                # Store fetch timestamp
                st.session_state.fetch_timestamp = datetime.now()
                
                # Store NWS source-reported update time if available
                if fetch_debug.get('nws_updated_parsed'):
                    st.session_state.nws_source_updated = fetch_debug['nws_updated_parsed']
                else:
                    st.session_state.nws_source_updated = None
            
            with st.spinner(f"🔍 Analyzing against {len(weather_features)} weather alerts + {len(outage_features)} outage zones..."):
                df_final = run_impact_analysis(
                    df_geo, 
                    weather_features, 
                    outage_features, 
                    enable_point_fallback=st.session_state.enable_fallback,
                    min_severity_rank=st.session_state.min_severity_rank,
                    exclude_low_priority=st.session_state.exclude_low_priority
                )
            
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
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Clients", len(df_final))

    at_risk_count = len(df_final[df_final['is_at_risk'] == True])
    col2.metric("Clients at Risk", at_risk_count, 
                delta=f"{at_risk_count}" if at_risk_count > 0 else None,
                delta_color="inverse")

    col3.metric("Weather Source", st.session_state.weather_source)

    valid_polys = geom_stats.get('valid_polygons', 0)
    total_feats = geom_stats.get('total_features', 0)
    col4.metric("Valid Polygons", f"{valid_polys}/{total_feats}")

    # Data Freshness Metric
    age_str, freshness_icon, is_stale = get_freshness_info(st.session_state.fetch_timestamp)
    col5.metric(f"{freshness_icon} Data Freshness", age_str)

    # --- Freshness Details Row ---
    if st.session_state.fetch_timestamp:
        fetch_time_str = st.session_state.fetch_timestamp.strftime('%I:%M:%S %p')
        nws_updated_str = st.session_state.nws_source_updated or "N/A"
        st.caption(f"🕐 **Fetched at:** {fetch_time_str} local | **NWS Source Updated:** {nws_updated_str}")

    # --- Staleness Warning ---
    if is_stale:
        st.warning(
            f"⏰ **Data may be stale** — Last fetched {age_str}. "
            "Weather conditions can change rapidly. Consider clicking **🔄 Run Spatial Analysis** to refresh with current data.",
            icon="⚠️"
        )

    # Info banner about merged sources
    if "Merged" in st.session_state.weather_source:
        st.info("ℹ️ **Multi-Source Mode**: Data merged from IEM + NWS to maximize alert coverage (erring on caution).")

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

    st_folium(m, width="100%", height=700, returned_objects=[])

    # --- Data Table ---
    st.subheader("Analysis Results")

    # Create a clean display dataframe with status emoji
    df_display = df_final.copy()
    df_display['Status'] = df_display['is_at_risk'].apply(
        lambda x: '🔴 AT RISK' if x else '🟢 Clear'
    )

    # Reorder columns for clarity
    display_cols = ['Status', 'ip', 'city', 'region', 'risk_details']
    if 'check_method' in df_display.columns:
        display_cols.append('check_method')

    # Filter out the boolean column since we have Status now
    st.dataframe(
        df_display[display_cols],
        use_container_width=True,
        hide_index=True
    )

    # Summary counts
    at_risk_df = df_final[df_final['is_at_risk'] == True]
    if not at_risk_df.empty:
        st.caption(f"⚠️ {len(at_risk_df)} of {len(df_final)} locations have active weather alerts meeting your threshold.")

    # --- Debug Expander ---
    with st.expander("🔧 Debug Information"):
        col_a, col_b, col_c = st.columns(3)
        
        with col_a:
            st.subheader("Fetch Statistics")
            st.json(st.session_state.fetch_debug)
        
        with col_b:
            st.subheader("Geometry Stats")
            st.json(st.session_state.geom_stats)
        
        with col_c:
            st.subheader("Filter Stats")
            st.json(st.session_state.get('filter_stats', {}))
        
        if geom_stats.get('null_geometry', 0) > 0:
            null_pct = (geom_stats['null_geometry'] / geom_stats['total_features']) * 100 if geom_stats['total_features'] > 0 else 0
            st.warning(f"⚠️ {null_pct:.1f}% of weather features have NULL geometry. "
                       "This is typical for NWS zone-based alerts. Point-API fallback recommended.")
        
        st.subheader("Sample Feature (Raw)")
        if weather_features:
            sample = weather_features[0]
            st.write("**Geometry Present:**", sample.get('geometry') is not None)
            st.write("**Geometry Type:**", sample.get('geometry', {}).get('type') if sample.get('geometry') else "NULL")
            st.write("**Properties:**")
            st.json(sample.get('properties', {}))
