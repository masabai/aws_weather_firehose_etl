import streamlit as st
import pandas as pd
import requests
import altair as alt
import json

# Page Configuration
st.set_page_config(page_title="HHS Region 9 Health Watch", layout="wide")

# API CONFIGURATION
API_URL = "https://3l6spjxqv5.execute-api.us-west-2.amazonaws.com"

@st.cache_data(ttl=60)
def get_data_from_api():
    """Fetches data and handles the AWS Lambda Proxy 'body' string wrapper."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        
        # Parse the top-level JSON from API Gateway
        api_response = response.json()
        
        # Extract the 'body' string (this is what changed in your AWS setup)
        # If 'body' doesn't exist, fall back to the whole response
        if isinstance(api_response, dict) and 'body' in api_response:
            raw_data = json.loads(api_response['body'])
        else:
            raw_data = api_response
            
        return pd.DataFrame(raw_data)
        
    except Exception as e:
        st.error(f"API Error: {e}")
        return pd.DataFrame()

# Sidebar & Global Controls
st.title("Weather & Flu Watch Alerts")
st.sidebar.header("Pipeline Status: LIVE")

if st.sidebar.button('Manual Refresh'):
    st.cache_data.clear()
    st.rerun()

# 4. Load Data
df = get_data_from_api()

if not df.empty:
    # Data Cleaning: Convert strings to numbers for visualization
    df['temp_current_f'] = pd.to_numeric(df['temp_current_f'], errors='coerce')
    df['cold_flu_index'] = pd.to_numeric(df['cold_flu_index'], errors='coerce').fillna(0)

    # KPI Metric Row
    col1, col2, col3 = st.columns(3)
    with col1:
        high_risk_count = len(df[df['flu_risk_category'] == 'High Risk'])
        st.metric("High Risk Alerts", high_risk_count)

    with col2:
        avg_temp = round(df['temp_current_f'].mean(), 1)
        st.metric("Avg Region Temp", f"{avg_temp}°F")

    with col3:
        # Format timestamp for display
        latest_ts = str(df['timestamp'].iloc[0])[:16].replace('T', ' ')
        st.write(f"**Last Sync (UTC):** {latest_ts}")

    # Visualization
    st.subheader("Regional Risk Breakdown")
    color_scale = alt.Scale(
        domain=['High Risk', 'Moderate Risk', 'Low Risk'],
        range=['#FF0000', '#0000FF', '#00FF00']
    )

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('location:N', title='City', sort='-y'),
        y=alt.Y('cold_flu_index:Q', title='Cold/Flu Index'),
        color=alt.Color('flu_risk_category:N', scale=color_scale, title='Risk Level'),
        tooltip=['location', 'state', 'cold_flu_index', 'flu_risk_category']
    ).properties(height=400)

    st.altair_chart(chart, use_container_width=True)

    st.subheader("Raw Health Data")
    # Using the [Streamlit Dataframe Guide](https://docs.streamlit.io)
    st.dataframe(df, use_container_width=True)
else:
    st.warning("Waiting for fresh data from API... Check Lambda Proxy Integration settings.")

