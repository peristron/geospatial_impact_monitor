import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
from shapely.geometry import shape, Point
from shapely.validation import make_valid
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from shapely.strtree import STRtree
import concurrent.futures

# --- CONFIGURATION ---
st.set_page_config(page_title="Geospatial Impact Monitor", layout="wide")

# --- PROJECTION OPTIONS FOR GLOBAL MAPPER ---
PROJECTION_OPTIONS = {
    # Compromise projections (balanced distortion)
    'Natural Earth': 'natural earth',
    'Robinson': 'robinson',
    'Winkel Tripel': 'winkel tripel',
    'Aitoff': 'aitoff',
    'Kavrayskiy VII': 'kavrayskiy7',
    
    # Cylindrical projections
    'Mercator': 'mercator',
    'Miller': 'miller',
    'Equirectangular': 'equirectangular',
    'Transverse Mercator': 'transverse mercator',
    
    # Azimuthal projections
    'Orthographic (Globe)': 'orthographic',
    'Azimuthal Equal Area': 'azimuthal equal area',
    'Azimuthal Equidistant': 'azimuthal equidistant',
    'Stereographic': 'stereographic',
    'Gnomonic': 'gnomonic',
    
    # Equal-area projections
    'Mollweide': 'mollweide',
    'Hammer': 'hammer',
    'Sinusoidal': 'sinusoidal',
    'Eckert IV': 'eckert4',
    
    # Conic projections
    'Conic Equal Area': 'conic equal area',
    'Conic Conformal': 'conic conformal',
    'Conic Equidistant': 'conic equidistant',
    
    # Special purpose
    'Albers USA': 'albers usa',
}

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

def geocode_bulk_nominatim(location_list):
    """
    Geocodes a list of place names using OpenStreetMap (Nominatim).
    Requires a User-Agent (already compliant).
    """
    url = "https://nominatim.openstreetmap.org/search"
    coords = []
    
    # Deduplicate
    location_list = list(filter(None, set(location_list)))
    
    # Progress bar wrapper could go here, but we'll keep it simple
    for loc in location_list:
        try:
            # Nominatim requires User-Agent
            headers = {'User-Agent': 'Geospatial-Impact-Monitor'}
            params = {'q': loc, 'format': 'json', 'limit': 1}
            
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data:
                    res = data[0]
                    coords.append({
                        'ip': loc, # Storing name in 'ip' column for compatibility with existing logic
                        'label': loc,
                        'lat': float(res['lat']), 
                        'lon': float(res['lon']), 
                        'city': loc, 
                        'region': res.get('display_name', ''),
                        'country': 'Geocoded'
                    })
                else:
                    # Not found
                    coords.append({'ip': loc, 'lat': None, 'lon': None, 'city': 'Not Found'})
            
            # RESPECT NOMINATIM POLICY: Max 1 request per second
            time.sleep(1.1) 
            
        except Exception:
            coords.append({'ip': loc, 'lat': None, 'lon': None, 'city': 'Error'})
            
    return pd.DataFrame(coords)


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
    seen_counties = set() # Track to avoid duplicates

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

def fetch_earthquakes():
    """
    Fetches USGS Earthquake data (Mag 2.5+ in last 24 hours).
    Returns GeoJSON features.
    """
    # USGS Feed: Magnitude 2.5+ earthquakes, past day
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            return r.json().get('features', [])
    except:
        pass
    return []

@st.cache_data(ttl=3600)
def fetch_wildfires():
    """
    Fetches current large wildfire perimeters (>500 acres) from NIFC.
    Source: National Interagency Fire Center (WFIGS)
    """
    # WFIGS Current Interagency Perimeters
    url = "https://services3.arcgis.com/T4QMspueLg7OBLS4/arcgis/rest/services/WFIGS_Interagency_Perimeters/FeatureServer/0/query"
    params = {
        'where': 'poly_Acres > 500',
        'outFields': 'poly_IncidentName,poly_Acres,attr_UniqueFireIdentifier',
        'f': 'geojson'
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return r.json().get('features', [])
    except:
        pass
    return []

@st.cache_data(ttl=3600, show_spinner=False)
def get_geolocation_bulk(ip_list):
    """
    Batched IP Geolocation with Caching.
    Cached for 1 hour to prevent API rate limits on re-runs.
    """
    url = "http://ip-api.com/batch"
    coords = []
    # Remove duplicates and empty strings
    ip_list = list(filter(None, set(ip_list)))

    # API limit is 15 requests per minute for batch endpoint
    chunk_size = 100
    
    for i in range(0, len(ip_list), chunk_size):
        chunk = ip_list[i:i + chunk_size]
        try:
            response = requests.post(url, json=chunk, timeout=10).json()
            
            # The API returns a list of dicts
            if isinstance(response, list):
                for res in response:
                    if res.get('status') == 'success':
                        coords.append({
                            'ip': res['query'], 
                            'lat': res['lat'], 
                            'lon': res['lon'], 
                            'city': res.get('city', 'N/A'), 
                            'region': res.get('regionName', res.get('region', 'N/A')),
                            'country': res.get('country', 'N/A'),
                            'countryCode': res.get('countryCode', 'N/A'),
                            'isp': res.get('isp', 'N/A'),
                            'org': res.get('org', 'N/A')
                        })
                    else:
                        # Log failed resolutions so we don't lose the row
                        coords.append({
                            'ip': res.get('query', 'Unknown'), 
                            'lat': None, 
                            'lon': None, 
                            'city': "N/A", 
                            'region': "N/A",
                            'country': "N/A",
                            'countryCode': "N/A",
                            'isp': "N/A",
                            'org': "N/A"
                        })
            
            # Respect rate limits (approx 1 req/sec is safe)
            time.sleep(1.0)
            
        except Exception as e:
            # If a chunk fails, we log it as None to avoid crashing
            for ip in chunk:
                coords.append({
                    'ip': ip, 
                    'lat': None, 
                    'lon': None, 
                    'city': "Error", 
                    'region': str(e),
                    'country': "N/A",
                    'countryCode': "N/A",
                    'isp': "N/A",
                    'org': "N/A"
                })

    return pd.DataFrame(coords)
def parse_coordinates_input(raw_text):
    """
    Parse coordinate input in various formats:
    - lat,lon (one per line)
    - lat lon (space separated)
    - lat;lon (semicolon separated)
    Returns DataFrame with lat, lon, and label columns
    """
    coords = []
    lines = raw_text.strip().split('\n')
    
    for idx, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        # Try different separators
        parts = None
        for sep in [',', ';', '\t', ' ']:
            if sep in line:
                parts = [p.strip() for p in line.split(sep) if p.strip()]
                if len(parts) >= 2:
                    break
        
        if parts and len(parts) >= 2:
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                label = parts[2] if len(parts) > 2 else f"Point {idx + 1}"
                
                # Validate ranges
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    coords.append({
                        'lat': lat,
                        'lon': lon,
                        'label': label
                    })
            except ValueError:
                continue
    
    return pd.DataFrame(coords)

# --- ANALYSIS ---

def run_impact_analysis(df_ips, weather_features, outage_features, earthquake_features=None, wildfire_features=None,
                        enable_point_fallback=True,
                        min_severity_rank=0,
                        exclude_low_priority=False):
    """
    Performs spatial intersection analysis using STRtree.
    Auto-detects Shapely version (2.0 returns indices, <2.0 returns geometries).
    """
    results = []
    weather_features = weather_features or []
    outage_features = outage_features or []
    earthquake_features = earthquake_features or []
    wildfire_features = wildfire_features or []

    # --- Helper: Prepare Features ---
    def prepare_polys(features, type_label):
        polys = []
        valid_count = 0
        for feature in features:
            geom = feature.get('geometry')
            props = feature.get('properties', {})
            
            if not geom or not geom.get('coordinates'):
                continue

            # Severity Filtering for Weather
            if type_label == 'weather':
                if not passes_severity_threshold(props, min_severity_rank, exclude_low_priority):
                    continue

            try:
                poly = shape(geom)
                if not poly.is_valid:
                    poly = make_valid(poly)
                
                # For Earthquakes, buffer the point to create a zone
                if type_label == 'earthquake':
                    poly = poly.buffer(0.5) # ~35 miles

                if poly.is_valid and not poly.is_empty:
                    desc = "Unknown"
                    if type_label == 'weather':
                        event_name = (props.get('event') or props.get('prod_type') or props.get('phenomena', 'WX'))
                        sev = props.get('severity', 'Unknown')
                        urgency = props.get('urgency', '')
                        desc = f"{event_name} ({sev}/{urgency})" if urgency else f"{event_name} ({sev})"
                    elif type_label == 'outage':
                        desc = f"Power Outage: {props.get('Percent_Out', 'N/A')}% - {props.get('NAME', 'Unknown')}"
                    elif type_label == 'earthquake':
                        mag = props.get('mag', 0)
                        place = props.get('place', 'Unknown')
                        time_str = datetime.fromtimestamp(props.get('time', 0)/1000).strftime('%Y-%m-%d %H:%M')
                        desc = f"Earthquake M{mag} near {place} ({time_str})"
                    elif type_label == 'wildfire':
                        name = props.get('poly_IncidentName', 'Unnamed Fire')
                        acres = props.get('poly_Acres', 0)
                        desc = f"Wildfire: {name} ({acres:,.0f} acres)"

                    polys.append({'poly': poly, 'desc': desc})
                    valid_count += 1
            except:
                continue
        return polys, valid_count

    # --- Build Lists & Trees ---
    weather_polys, w_count = prepare_polys(weather_features, 'weather')
    outage_polys, o_count = prepare_polys(outage_features, 'outage')
    earthquake_polys, e_count = prepare_polys(earthquake_features, 'earthquake')
    wildfire_polys, f_count = prepare_polys(wildfire_features, 'wildfire')

    # Update Stats
    st.session_state.geom_stats = {
        'total_features': len(weather_features), 
        'valid_polygons': w_count
    }

    # Build Trees
    w_geoms = [p['poly'] for p in weather_polys]
    o_geoms = [p['poly'] for p in outage_polys]
    e_geoms = [p['poly'] for p in earthquake_polys]
    f_geoms = [p['poly'] for p in wildfire_polys]
    
    weather_tree = STRtree(w_geoms) if w_geoms else None
    outage_tree = STRtree(o_geoms) if o_geoms else None
    earthquake_tree = STRtree(e_geoms) if e_geoms else None
    wildfire_tree = STRtree(f_geoms) if f_geoms else None

    # Determine Fallback
    use_point_fallback = (enable_point_fallback and w_count < len(weather_features) * 0.1 and len(weather_features) > 0)
    st.session_state.using_point_fallback = use_point_fallback

    # --- Analyze IPs ---
    for index, row in df_ips.iterrows():
        hazards = []
        is_at_risk = False
        check_method = "polygon"
        
        if pd.notnull(row.get('lat')) and pd.notnull(row.get('lon')):
            point = Point(row['lon'], row['lat'])
            point_buffer = point.buffer(0.001)

            # Helper to query tree safely
            def query_tree(tree, source_polys):
                found_hazards = []
                if not tree: return []
                result = tree.query(point)
                
                indices = []
                if len(result) > 0:
                    if isinstance(result[0], int) or (hasattr(result, 'dtype') and 'int' in str(result.dtype)):
                        indices = result
                    else:
                        for geom in result:
                            for item in source_polys:
                                if item['poly'] == geom:
                                    if item['poly'].contains(point) or item['poly'].intersects(point_buffer):
                                        found_hazards.append(item['desc'])
                        return found_hazards

                for idx in indices:
                    item = source_polys[idx]
                    if item['poly'].contains(point) or item['poly'].intersects(point_buffer):
                        found_hazards.append(item['desc'])
                return found_hazards

            # Run Checks
            hazards.extend(query_tree(weather_tree, weather_polys))
            hazards.extend(query_tree(outage_tree, outage_polys))
            hazards.extend(query_tree(earthquake_tree, earthquake_polys))
            hazards.extend(query_tree(wildfire_tree, wildfire_polys))

            if hazards:
                is_at_risk = True

            # Fallback
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

# --- GLOBAL MAPPER VISUALIZATION ---

def create_global_map(df_locations, projection='natural earth', 
                      marker_size=10, marker_color='#FF6B6B',
                      show_labels=True, globe_rotation=None):
    """
    Create a Plotly map with the specified projection.
    Returns a Plotly figure object.
    """
    if df_locations.empty:
        # Return empty map
        fig = go.Figure(go.Scattergeo())
        fig.update_layout(
            geo=dict(projection_type=projection),
            title="No locations to display"
        )
        return fig
    
    # Determine label column
    if 'label' in df_locations.columns:
        labels = df_locations['label']
    elif 'ip' in df_locations.columns:
        labels = df_locations['ip']
    else:
        labels = [f"Point {i+1}" for i in range(len(df_locations))]
    
    # Build hover text
    hover_texts = []
    for idx, row in df_locations.iterrows():
        parts = []
        if 'label' in row and pd.notnull(row.get('label')):
            parts.append(f"<b>{row['label']}</b>")
        if 'ip' in row and pd.notnull(row.get('ip')):
            parts.append(f"IP: {row['ip']}")
        if 'city' in row and row.get('city') != 'N/A':
            parts.append(f"City: {row['city']}")
        if 'region' in row and row.get('region') != 'N/A':
            parts.append(f"Region: {row['region']}")
        if 'country' in row and row.get('country') != 'N/A':
            parts.append(f"Country: {row['country']}")
        parts.append(f"Lat: {row['lat']:.4f}")
        parts.append(f"Lon: {row['lon']:.4f}")
        if 'isp' in row and row.get('isp') != 'N/A':
            parts.append(f"ISP: {row['isp']}")
        hover_texts.append("<br>".join(parts))
    
    # Create the scatter geo plot
    fig = go.Figure()
    
    fig.add_trace(go.Scattergeo(
        lon=df_locations['lon'],
        lat=df_locations['lat'],
        mode='markers+text' if show_labels else 'markers',
        marker=dict(
            size=marker_size,
            color=marker_color,
            opacity=0.8,
            line=dict(width=1, color='white')
        ),
        text=labels if show_labels else None,
        textposition='top center',
        textfont=dict(size=9, color='#333'),
        hoverinfo='text',
        hovertext=hover_texts,
        name='Locations'
    ))
    
    # Configure the geo layout
    geo_config = dict(
        projection_type=projection,
        showland=True,
        landcolor='rgb(243, 243, 243)',
        countrycolor='rgb(204, 204, 204)',
        coastlinecolor='rgb(150, 150, 150)',
        showocean=True,
        oceancolor='rgb(230, 245, 255)',
        showlakes=True,
        lakecolor='rgb(200, 230, 255)',
        showcountries=True,
        showcoastlines=True,
        showframe=True,
        framecolor='rgb(150, 150, 150)',
    )
    
    # Handle globe rotation for orthographic projection
    if projection == 'orthographic' and globe_rotation:
        geo_config['projection_rotation'] = globe_rotation
    
    fig.update_layout(
        geo=geo_config,
        margin=dict(l=0, r=0, t=30, b=0),
        height=600,
        showlegend=False
    )
    
    return fig

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
if 'earthquake_data' not in st.session_state:
    st.session_state.earthquake_data = None
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
    st.session_state.min_severity_rank = 2 # Default to Moderate+
if 'exclude_low_priority' not in st.session_state:
    st.session_state.exclude_low_priority = True
if 'fetch_timestamp' not in st.session_state:
    st.session_state.fetch_timestamp = None
if 'nws_source_updated' not in st.session_state:
    st.session_state.nws_source_updated = None

# Global Mapper session state
if 'global_mapper_data' not in st.session_state:
    st.session_state.global_mapper_data = None
if 'global_mapper_projection' not in st.session_state:
    st.session_state.global_mapper_projection = 'natural earth'

# --- UI ---

st.title("🌍 Geospatial Tools Suite")

# --- MODE SELECTION TABS ---
tab_impact, tab_mapper = st.tabs(["🌩️ Impact Monitor (Multi-Hazard)", "🗺️ Global Location Mapper"])

# --- CALLBACK FUNCTION FOR FILTER CHANGES ---

def rerun_analysis_with_filters():
    """Re-run analysis with current filter settings using cached data."""
    if (st.session_state.geo_data is not None and
        st.session_state.weather_data is not None):

        quakes = st.session_state.get('earthquake_data', [])
        fires = st.session_state.get('wildfire_data', [])

        df_final = run_impact_analysis(
            st.session_state.geo_data,
            st.session_state.weather_data,
            st.session_state.outage_data,
            earthquake_features=quakes,
            wildfire_features=fires,
            enable_point_fallback=st.session_state.enable_fallback,
            min_severity_rank=st.session_state.min_severity_rank,
            exclude_low_priority=st.session_state.exclude_low_priority
        )
        st.session_state.analysis_results = df_final

# ============================================================================
# TAB 1: IMPACT MONITOR (Original Functionality)
# ============================================================================

with tab_impact:
    st.markdown("**How it works:** Enter IP addresses (from clients, users, or devices) to assess risks from active **weather alerts, power outages, earthquakes, and wildfires**. The app geolocates each IP, then checks against ~real-time data from **NOAA, USGS, NIFC, and utility providers** using zone intersections and targeted queries for accurate impact detection. The engine 'errs on the side of caution' to prioritize safety.")
    with st.sidebar:
        st.header("🌩️ Impact Monitor Controls")
        st.caption("Settings for the **Impact Monitor** tab")
        st.subheader("Data Input")
        input_method = st.radio("Method", ["Paste IP List", "Bulk Upload"], key="impact_input_method")
        ip_list = []

        if input_method == "Paste IP List":
            # Default IPs: higher-ed and/or k12 educational institutions using D2L Brightspace in the US South/Southeast
            raw_input = st.text_area(
                "Paste IPs (1 per line or comma-separated)", 
                "152.97.17.168\n34.236.193.193\n24.199.66.32\n141.193.213.20\n23.185.0.2\n104.153.195.192\n128.23.35.87\n104.17.163.123", 
                height=150,
                key="impact_ip_input"
            )
            if raw_input: 
                ip_list = [ip.strip() for ip in raw_input.replace(',', '\n').split('\n') if ip.strip()]
        else:
            uploaded_file = st.file_uploader("Upload CSV/XLSX", type=['csv', 'xlsx'], key="impact_file_upload")
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
        
        # updates session state and trigger re-analysis if changed
        if enable_fallback != st.session_state.enable_fallback:
            st.session_state.enable_fallback = enable_fallback
            rerun_analysis_with_filters()

        if st.button("🔄 Run Spatial Analysis", type="primary", key="run_impact_analysis"):
            if ip_list:
                # clear cache for fresh data
                st.cache_data.clear()
                
                with st.spinner("📍 Geolocating IPs..."):
                    df_geo = get_geolocation_bulk(ip_list)
                    st.session_state.geo_data = df_geo
                
                with st.spinner("🔥 Fetching Weather, Power, Quakes & Wildfires..."):
                    def run_parallel_fetches():
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future_weather = executor.submit(fetch_weather_data_hybrid)
                            future_outage = executor.submit(fetch_power_outages)
                            future_quakes = executor.submit(fetch_earthquakes)
                            future_fires = executor.submit(fetch_wildfires)
                            
                            return (future_weather.result(), future_outage.result(), 
                                    future_quakes.result(), future_fires.result())

                    ((weather_features, source_name, fetch_debug), 
                     outage_features, earthquake_features, wildfire_features) = run_parallel_fetches()

                    st.session_state.fetch_timestamp = datetime.now()
                    st.session_state.nws_source_updated = fetch_debug.get('nws_updated_parsed')
                
                with st.spinner(f"🔍 Cross-referencing locations against {len(weather_features)} weather alerts, {len(outage_features)} power outages, {len(earthquake_features)} earthquakes, and {len(wildfire_features)} wildfires..."):
                    df_final = run_impact_analysis(
                        df_geo, 
                        weather_features, 
                        outage_features,
                        earthquake_features=earthquake_features,
                        wildfire_features=wildfire_features,
                        enable_point_fallback=st.session_state.enable_fallback,
                        min_severity_rank=st.session_state.min_severity_rank,
                        exclude_low_priority=st.session_state.exclude_low_priority
                    )
                
                st.session_state.analysis_results = df_final
                st.session_state.weather_data = weather_features
                st.session_state.weather_source = source_name
                st.session_state.fetch_debug = fetch_debug
                st.session_state.outage_data = outage_features
                st.session_state.earthquake_data = earthquake_features
                st.session_state.wildfire_data = wildfire_features
                
                st.success("Analysis complete!")
            else:
                st.warning("Please input at least one IP address.")

    # --- DISPLAY RESULTS ---

    if st.session_state.analysis_results is not None:
        df_final = st.session_state.analysis_results
        weather_features = st.session_state.weather_data or []
        outage_features = st.session_state.outage_data or []
        wildfire_features = st.session_state.get('wildfire_data', [])
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

        age_str, freshness_icon, is_stale = get_freshness_info(st.session_state.fetch_timestamp)
        col5.metric(f"{freshness_icon} Data Freshness", age_str)

        st.caption(
            "**Metrics:** Clients at Risk = IPs in active alert/outage zones · "
            "Valid Polygons = mappable alert boundaries (0 is OK if Point-API fallback is enabled)"
        )

        with st.expander("ℹ️ What do these metrics mean?"):
            st.markdown("""
| Metric | Description |
| :--- | :--- |
| **Total Clients** | Count of unique IP addresses that were successfully geolocated. |
| **Clients at Risk** | Locations that fall within an active weather alert, outage, quake, or wildfire zone. |
| **Weather Source** | Where alert data was fetched from. |
| **Valid Polygons** | How many alerts have geographic boundaries that can be mapped. |
| **Data Freshness** | Time since alert data was fetched. |
            """)

        if st.session_state.fetch_timestamp:
            fetch_time_str = st.session_state.fetch_timestamp.strftime('%I:%M:%S %p')
            nws_updated_str = st.session_state.nws_source_updated or "N/A"
            st.caption(f"🕐 **Fetched at:** {fetch_time_str} local | **NWS Source Updated:** {nws_updated_str}")

        if is_stale:
            st.warning(f"⏰ **Data may be stale** — Last fetched {age_str}.", icon="⚠️")

        if "Merged" in st.session_state.weather_source:
            st.info("ℹ️ **Multi-Source Mode**: Data merged from IEM + NWS.")

        if st.session_state.using_point_fallback:
            st.warning("⚠️ **Point-API Fallback Active**: Most weather alerts lack polygon geometry. Using direct NWS point queries.")

        # --- Strategic Impact Assessment ---
        st.subheader("🧠 Strategic Impact Assessment")
        
        weather_confinement_count = 0
        probable_offline_count = 0

        for _, row in df_final.iterrows():
            if row['is_at_risk']:
                details = str(row.get('risk_details', ''))
                if "Power Outage" in details or "Earthquake" in details:
                    probable_offline_count += 1
                else:
                    weather_confinement_count += 1

        with st.container(border=True):
            st.markdown("""
            **Service Provider Context:** Severe weather typically increases online traffic, 
            while power outages and earthquakes cause immediate traffic drops.
            """)
            c1, c2 = st.columns(2)
            c1.metric("📈 Potential Usage Spike (High Load)", f"{weather_confinement_count} Clients")
            c2.metric("📉 Probable Traffic Drop (Offline)", f"{probable_offline_count} Clients")

        # --- Map with Layer Controls ---
        st.subheader("Interactive Threat Map")
        
        if not df_final.empty and pd.notnull(df_final.iloc[0].get('lat')):
            center_lat = df_final['lat'].mean()
            center_lon = df_final['lon'].mean()
        else:
            center_lat, center_lon = 39.8283, -98.5795
            
        m = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles='CartoDB positron')
        
        # Create FeatureGroups for Layer Control
        fg_outages = folium.FeatureGroup(name="Power Outages", show=True)
        fg_weather = folium.FeatureGroup(name="Weather Alerts", show=True)
        fg_fires = folium.FeatureGroup(name="Wildfires", show=True)
        fg_clients = folium.FeatureGroup(name="Clients (IPs)", show=True)

        # Layer: Power Outages
        for feat in outage_features:
            try:
                style = lambda x: {'fillColor': '#111111', 'color': 'black', 'fillOpacity': 0.5, 'weight': 1}
                props = feat.get('properties', {})
                tooltip = f"Outage: {props.get('NAME', 'Unknown')} - {props.get('Percent_Out', '?')}%"
                folium.GeoJson(feat, style_function=style, tooltip=tooltip).add_to(fg_outages)
            except: continue
        
        # Layer: Wildfires
        for feat in wildfire_features:
            try:
                style = lambda x: {'fillColor': '#FF4500', 'color': '#8B0000', 'fillOpacity': 0.6, 'weight': 2}
                props = feat.get('properties', {})
                tooltip = f"🔥 {props.get('poly_IncidentName', 'Fire')} ({props.get('poly_Acres', 0):,.0f} acres)"
                folium.GeoJson(feat, style_function=style, tooltip=tooltip).add_to(fg_fires)
            except: continue

        # Layer: Weather Polygons
        for feat in weather_features:
            geom = feat.get('geometry')
            if not geom or not geom.get('coordinates'): continue
            props = feat.get('properties', {})
            desc = str(props).lower()
            
            color, opacity = '#808080', 0.3
            if 'tornado' in desc or "'to'" in desc: color, opacity = '#FF0000', 0.7
            elif 'thunderstorm' in desc or 'severe' in desc: color, opacity = '#FFA500', 0.5
            elif 'flood' in desc: color, opacity = '#228B22', 0.5
            elif 'winter' in desc or 'snow' in desc: color, opacity = '#1E90FF', 0.5
            elif 'heat' in desc: color, opacity = '#FF4500', 0.5
            elif 'wind' in desc: color, opacity = '#9370DB', 0.4
            
            try:
                style = lambda x, c=color, o=opacity: {'fillColor': c, 'color': c, 'fillOpacity': o, 'weight': 1}
                tooltip = props.get('event') or props.get('prod_type') or 'Weather Alert'
                folium.GeoJson(feat, style_function=style, tooltip=tooltip).add_to(fg_weather)
            except: continue
                
        # Layer: IP Markers
        for _, row in df_final.iterrows():
            if pd.notnull(row.get('lat')):
                is_risk = row.get('is_at_risk', False)
                details = str(row.get('risk_details', '')).lower()
                
                # Default Safe State
                color = 'green'
                icon = 'check'
                
                # Dynamic Hazard Icons (Priority Order)
                if is_risk:
                    if 'wildfire' in details:
                        color = 'darkred'
                        icon = 'fire'
                    elif 'power outage' in details:
                        color = 'black'
                        icon = 'plug'
                    elif 'earthquake' in details:
                        color = 'orange'
                        icon = 'house-crack' # or 'activity'
                    elif 'tornado' in details:
                        color = 'red'
                        icon = 'wind' 
                    elif 'thunderstorm' in details or 'storm' in details:
                        color = 'red'
                        icon = 'cloud-bolt'
                    elif 'flood' in details:
                        color = 'blue'
                        icon = 'water'
                    elif 'winter' in details or 'snow' in details or 'ice' in details:
                        color = 'blue'
                        icon = 'snowflake'
                    elif 'heat' in details:
                        color = 'orange'
                        icon = 'temperature-high'
                    else:
                        # Generic Warning
                        color = 'red'
                        icon = 'triangle-exclamation'

                popup_html = f"<b>IP:</b> {row.get('ip')}<br><b>Status:</b> {'⚠️ AT RISK' if is_risk else '✅ Clear'}<br><b>Details:</b> {row.get('risk_details')}"
                
                folium.Marker(
                    [row['lat'], row['lon']], 
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color=color, icon=icon, prefix='fa')
                ).add_to(fg_clients)

        # Add all layers to map
        fg_outages.add_to(m)
        fg_weather.add_to(m)
        fg_fires.add_to(m)
        fg_clients.add_to(m)
        
        # Add Layer Control
        folium.LayerControl().add_to(m)

        st_folium(m, width="100%", height=700, returned_objects=[])

        # --- Data Table & Export ---
        st.subheader("Analysis Results")
        
        df_display = df_final.copy()
        df_display['Status'] = df_display['is_at_risk'].apply(lambda x: '🔴 AT RISK' if x else '🟢 Clear')
        display_cols = ['Status', 'ip', 'city', 'region', 'risk_details']
        if 'check_method' in df_display.columns: display_cols.append('check_method')
        
        st.dataframe(df_display[display_cols], use_container_width=True, hide_index=True)

        # CSV Download Button
        csv = df_final.to_csv(index=False)
        st.download_button(
            label="📥 Download Analysis Results (CSV)",
            data=csv,
            file_name=f"impact_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
        
        at_risk_df = df_final[df_final['is_at_risk'] == True]
        if not at_risk_df.empty:
            st.caption(f"⚠️ {len(at_risk_df)} of {len(df_final)} locations have active risks.")

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
    else:
        st.info("👈 Enter IP addresses in the sidebar and click **Run Spatial Analysis** to begin.")

# ============================================================================
# TAB 2: GLOBAL LOCATION MAPPER
# ============================================================================

with tab_mapper:
    st.markdown("**Map any location worldwide** with support for multiple map projections. Input IP addresses (auto-geolocated) or direct coordinates.")
    
    # --- Sidebar for Global Mapper ---
    with st.sidebar:
        st.divider()
        st.header("🗺️ Global Mapper Controls")
        st.caption("Settings for the **Global Location Mapper** tab")
        
        # Input type selection
        mapper_input_type = st.radio(
            "Input Type",
            ["IP Addresses", "Coordinates", "Upload File"],
            key="mapper_input_type"
        )
        
        mapper_locations = pd.DataFrame()
        
        if mapper_input_type == "IP Addresses":
            mapper_ip_input = st.text_area(
                "Paste IPs (1 per line)",
                "8.8.8.8\n1.1.1.1\n208.67.222.222\n9.9.9.9\n185.228.168.9",
                height=120,
                key="mapper_ip_input",
                help="Enter IP addresses to geolocate and map"
            )
            
        elif mapper_input_type == "Coordinates":
            mapper_coord_input = st.text_area(
                "Paste coordinates (lat,lon per line)",
                "51.5074,-0.1278,London\n48.8566,2.3522,Paris\n35.6762,139.6503,Tokyo\n-33.8688,151.2093,Sydney\n40.7128,-74.0060,New York",
                height=120,
                key="mapper_coord_input",
                help="Format: lat,lon or lat,lon,label"
            )
        else:
            mapper_file = st.file_uploader(
                "Upload CSV/XLSX",
                type=['csv', 'xlsx'],
                key="mapper_file_upload",
                help="File should have 'lat' and 'lon' columns (or 'ip' column)"
            )
        
        st.divider()
        st.subheader("🎨 Map Settings")
        
        # Projection selection
        selected_projection_name = st.selectbox(
            "Map Projection",
            options=list(PROJECTION_OPTIONS.keys()),
            index=0,
            key="projection_select",
            help="Different projections show the Earth in different ways"
        )
        selected_projection = PROJECTION_OPTIONS[selected_projection_name]
        
        # Projection info
        projection_info = {
            # Compromise
            'natural earth': "Balanced compromise projection, good for world maps",
            'robinson': "Compromise projection, widely used for world maps",
            'winkel tripel': "Compromise projection used by National Geographic",
            'aitoff': "Modified azimuthal, good for world maps",
            'kavrayskiy7': "Compromise projection with minimal distortion",
            
            # Cylindrical
            'mercator': "Preserves angles, distorts size at poles. Standard web map projection",
            'miller': "Modified Mercator with less polar distortion",
            'equirectangular': "Simple lat/lon grid, preserves distances along equator",
            'transverse mercator': "Accurate for narrow north-south regions (UTM zones)",
            
            # Azimuthal
            'orthographic': "3D globe appearance, shows Earth as seen from space",
            'azimuthal equal area': "Preserves area from center point outward",
            'azimuthal equidistant': "Preserves distances from center — ideal for range/radius maps",
            'stereographic': "Conformal (preserves shapes), used for polar regions",
            'gnomonic': "Great circles appear as straight lines — shows shortest paths",
            
            # Equal-area
            'mollweide': "Equal-area, good for showing global distributions",
            'hammer': "Equal-area, elliptical shape, similar to Mollweide",
            'sinusoidal': "Equal-area, preserves distances along parallels",
            'eckert4': "Equal-area with rounded corners, popular for thematic maps",
            
            # Conic
            'conic equal area': "Equal-area, good for mid-latitude regions",
            'conic conformal': "Preserves angles, used for aeronautical charts",
            'conic equidistant': "Preserves distances along meridians",
            
            # Special
            'albers usa': "Optimized specifically for contiguous US + Alaska + Hawaii",
        }
        st.caption(projection_info.get(selected_projection, ""))
        

        # Center point rotation for azimuthal projections
        globe_rotation = None
        azimuthal_projections = ['orthographic', 'azimuthal equal area', 'azimuthal equidistant', 'stereographic', 'gnomonic']
        
        if selected_projection in azimuthal_projections:
            if selected_projection == 'orthographic':
                st.markdown("**Globe View Center**")
            else:
                st.markdown("**Projection Center Point**")
                st.caption("Distances/areas measured from this point")
            
            # Initialize default values if not set
            if 'center_lon' not in st.session_state:
                st.session_state.center_lon = 0
            if 'center_lat' not in st.session_state:
                st.session_state.center_lat = 20
            
            # Callback functions for presets (run BEFORE widget render on next rerun)
            def set_preset_nyc():
                st.session_state.center_lon = -74
                st.session_state.center_lat = 41
            
            def set_preset_london():
                st.session_state.center_lon = 0
                st.session_state.center_lat = 51
            
            def set_preset_tokyo():
                st.session_state.center_lon = 140
                st.session_state.center_lat = 36
            
            # Quick presets (placed BEFORE sliders so callbacks set values before slider renders)
            st.markdown("**Quick Presets:**")
            preset_cols = st.columns(3)
            with preset_cols[0]:
                st.button("🗽 NYC", key="preset_nyc", on_click=set_preset_nyc)
            with preset_cols[1]:
                st.button("🗼 London", key="preset_london", on_click=set_preset_london)
            with preset_cols[2]:
                st.button("🗻 Tokyo", key="preset_tokyo", on_click=set_preset_tokyo)
            
            # Sliders use session state values (updated by callbacks on previous run)
            rot_lon = st.slider(
                "Center Longitude", -180, 180, 
                value=st.session_state.center_lon,
                key="slider_rot_lon"
            )
            rot_lat = st.slider(
                "Center Latitude", -90, 90, 
                value=st.session_state.center_lat,
                key="slider_rot_lat"
            )
            
            # Update session state when sliders change manually
            st.session_state.center_lon = rot_lon
            st.session_state.center_lat = rot_lat
            
            globe_rotation = dict(lon=rot_lon, lat=rot_lat)                    
        st.divider()
        
        # Marker settings
        marker_size = st.slider("Marker Size", 5, 25, 12, key="marker_size")
        marker_color = st.color_picker("Marker Color", "#FF6B6B", key="marker_color")
        show_labels = st.checkbox("Show Labels", value=True, key="show_labels")
        
        # Map button
        if st.button("🗺️ Generate Map", type="primary", key="generate_map"):
            with st.spinner("Processing locations..."):
                if mapper_input_type == "IP Addresses":
                    ip_list = [ip.strip() for ip in mapper_ip_input.split('\n') if ip.strip()]
                    if ip_list:
                        # Use cached function and copy result to avoid mutation warnings
                        raw_data = get_geolocation_bulk(ip_list)
                        mapper_locations = raw_data.copy() if not raw_data.empty else raw_data
                        
                        if not mapper_locations.empty:
                            mapper_locations['label'] = mapper_locations.apply(
                                lambda r: f"{r['city']}, {r['countryCode']}" if r['city'] != 'N/A' else r['ip'],
                                axis=1
                            )
                        
                elif mapper_input_type == "Coordinates":
                    mapper_locations = parse_coordinates_input(mapper_coord_input)
                    
                else:  # File upload
                    if mapper_file:
                        try:
                            if mapper_file.name.endswith('.csv'):
                                df = pd.read_csv(mapper_file)
                            else:
                                df = pd.read_excel(mapper_file)
                            
                            # Normalize columns
                            cols = {c.lower(): c for c in df.columns}
                            lat_col = next((cols[c] for c in cols if 'lat' in c), None)
                            lon_col = next((cols[c] for c in cols if 'lon' in c or 'lng' in c), None)
                            ip_col = next((cols[c] for c in cols if 'ip' in c), None)
                            
                            if lat_col and lon_col:
                                mapper_locations = df.rename(columns={lat_col: 'lat', lon_col: 'lon'})
                                if 'label' not in mapper_locations.columns:
                                    name_col = next((cols[c] for c in cols if 'name' in c), None)
                                    if name_col:
                                        mapper_locations['label'] = mapper_locations[name_col]
                                    else:
                                        mapper_locations['label'] = [f"Point {i+1}" for i in range(len(mapper_locations))]
                            elif ip_col:
                                ip_list = df[ip_col].astype(str).tolist()
                                raw_data = get_geolocation_bulk(ip_list)
                                mapper_locations = raw_data.copy() if not raw_data.empty else raw_data
                                if not mapper_locations.empty:
                                    mapper_locations['label'] = mapper_locations.apply(
                                        lambda r: f"{r['city']}, {r['countryCode']}" if r['city'] != 'N/A' else r['ip'],
                                        axis=1
                                    )
                        except Exception as e:
                            st.error(f"Error reading file: {e}")
                
                # Filter valid
                if not mapper_locations.empty and 'lat' in mapper_locations.columns:
                    mapper_locations = mapper_locations.dropna(subset=['lat', 'lon'])
                
                # Update Session State
                st.session_state.global_mapper_data = mapper_locations
                st.session_state.global_mapper_projection = selected_projection

            # --- AUTO-SWITCH TO TAB 2 (JavaScript) ---
            # This script simulates a click on the second tab (Index 1)
            # It runs silently in the background
            js_switch_tab = """
            <script>
                var tabs = window.parent.document.querySelectorAll('button[data-baseweb="tab"]');
                if (tabs.length > 1) {
                    tabs[1].click();
                }
            </script>
            """
            components.html(js_switch_tab, height=0)

    # --- Display Global Map ---
    # Use Session State data AND Session State projection (ensures map matches the button click)
    if st.session_state.global_mapper_data is not None and not st.session_state.global_mapper_data.empty:
        df_map = st.session_state.global_mapper_data
        # Use the projection saved in session state (from the button click), NOT the widget
        current_proj = st.session_state.global_mapper_projection
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Locations", len(df_map))
        
        country_col = 'country' if 'country' in df_map.columns else 'countryCode'
        if country_col in df_map.columns:
            col2.metric("Countries", df_map[country_col].nunique())
        
        # Display the projection name based on the value in session state
        proj_name = next((k for k, v in PROJECTION_OPTIONS.items() if v == current_proj), "Unknown")
        col3.metric("Projection", proj_name)
        
        # Create and display the map
        fig = create_global_map(
            df_map,
            projection=current_proj, # <--- KEY FIX: Uses session state, not widget
            marker_size=marker_size,
            marker_color=marker_color,
            show_labels=show_labels,
            globe_rotation=globe_rotation
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("📍 Location Data")
        display_cols = [c for c in ['label', 'ip', 'lat', 'lon', 'city', 'region', 'country', 'countryCode', 'isp'] if c in df_map.columns]
        st.dataframe(df_map[display_cols], use_container_width=True, hide_index=True)
        
        # Export
        st.download_button(
            "📥 Download as CSV",
            df_map.to_csv(index=False),
            "geolocations.csv",
            "text/csv"
        )
    else:
        st.info("👈 Enter locations in the sidebar and click **Generate Map** to visualize.")
