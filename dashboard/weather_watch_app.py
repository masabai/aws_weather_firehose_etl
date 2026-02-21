import streamlit as st
import pandas as pd
import requests
import altair as alt

# 1. Page Configuration
st.set_page_config(page_title="HHS Region 9 Health Watch", layout="wide")

# 2. API CONFIGURATION (FIXED: Added the /weather_api_proxy route to avoid 404)
API_URL = "https://3l6spjxqv5.execute-api.us-west-2.amazonaws.com"

@st.cache_data(ttl=60)
def get_data_from_api():
    """Fetches deduplicated Gold data from the Secure API Proxy."""
    try:
        # Added timeout to prevent hanging
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return pd.DataFrame()
            
        return pd.DataFrame(data)
    except Exception as e:
        # This will show the error details if the API is down
        st.error(f"API Error: {e}")
        return pd.DataFrame()

# 3. Sidebar & Global Controls
st.title("Weather & Flu Watch Alerts")
st.sidebar.header("Pipeline Status: LIVE")

if st.sidebar.button('Manual Refresh'):
    st.cache_data.clear()
    st.rerun()

# 4. Load Data
df = get_data_from_api()

# 5. UI Logic
if not df.empty:
    # KPI Metric Row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'flu_risk_category' in df.columns:
            high_risk_count = len(df[df['flu_risk_category'] == 'High Risk'])
            st.metric("High Risk Alerts", high_risk_count)
        else:
            st.metric("High Risk Alerts", "N/A")

    with col2:
        if 'temp_current_f' in df.columns:
            df['temp_current_f'] = pd.to_numeric(df['temp_current_f'], errors='coerce')
            avg_temp = round(df['temp_current_f'].mean(), 1)
            st.metric("Avg Region Temp", f"{avg_temp}°F")
        else:
            st.metric("Avg Region Temp", "N/A")

    with col3:
        if 'timestamp' in df.columns:
            latest_ts = str(df['timestamp'].iloc[0])[:16].replace('T', ' ')
            st.write(f"**Last Sync (UTC):** {latest_ts}")
        else:
            st.write("**Last Sync:** Unknown")

    # Visualization
    st.subheader("Regional Risk Breakdown")
    
    color_scale = alt.Scale(
        domain=['High Risk', 'Moderate Risk', 'Low Risk'],
        range=['#FF0000', '#0000FF', '#00FF00']
    )

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('location:N', title='City'),
        y=alt.Y('cold_flu_index:Q', title='Cold/Flu Index'),
        color=alt.Color('flu_risk_category:N', scale=color_scale, title='Risk Level'),
        tooltip=['location', 'state', 'cold_flu_index', 'flu_risk_category']
    ).properties(height=400)

    # Replaced deprecated use_container_width
    st.altair_chart(chart, width="stretch")

    st.subheader("Raw Health Data")
    # Replaced deprecated use_container_width
    st.dataframe(df, width="stretch")

else:
    st.warning("No data returned from API. Check the debug info below.")
    
    with st.expander("Developer Debug Info"):
        st.write(f"Connecting to: {API_URL}")
        try:
            test_res = requests.get(API_URL, timeout=5)
            st.write(f"Status Code: {test_res.status_code}")
            st.write("Raw Output:", test_res.text)
        except Exception as debug_e:
            st.write(f"Connection Test Failed: {debug_e}")
