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
    """
    Fetches data and handles the AWS Lambda Proxy Integration 'body' string wrapper.
    If it worked 2 weeks ago and broke now, the API is likely wrapping the data 
    in a stringified 'body' key due to a Lambda Proxy Integration setting.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        
        # 1. Parse the top-level JSON from API Gateway
        res_data = response.json()
        
        # 2. Extract the 'body'. In Proxy mode, AWS sends the list as a STRING inside 'body'
        # We must use json.loads() a SECOND time to turn that string into a list.
        if isinstance(res_data, dict) and 'body' in res_data:
            inner_body = res_data['body']
            # If the body is a string, parse it into a list; otherwise use it as is
            final_data = json.loads(inner_body) if isinstance(inner_body, str) else inner_body
        elif isinstance(res_data, str):
            # Handle cases where the entire response is stringified
            final_data = json.loads(res_data)
        else:
            final_data = res_data
            
        return pd.DataFrame(final_data)
        
    except Exception as e:
        st.error(f"API Error: {e}")
        # If it fails, show the raw text so we can see if Athena or IAM failed
        if 'response' in locals():
            st.write("Raw API Output for Debugging:", response.text)
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
    # --- DATA CLEANING ---
    # Convert numbers sent as strings (from Athena/JSON) into actual floats/ints
    df['temp_current_f'] = pd.to_numeric(df['temp_current_f'], errors='coerce')
    df['cold_flu_index'] = pd.to_numeric(df['cold_flu_index'], errors='coerce').fillna(0)

    # KPI Metric Row
    col1, col2, col3 = st.columns(3)
    with col1:
        # Check for High Risk alerts
        high_risk_count = len(df[df['flu_risk_category'] == 'High Risk'])
        st.metric("High Risk Alerts", high_risk_count)

    with col2:
        # Calculate average temperature across the region
        avg_temp = round(df['temp_current_f'].mean(), 1)
        st.metric("Avg Region Temp", f"{avg_temp}°F")

    with col3:
        # Display the timestamp from the first record
        try:
            latest_ts = str(df['timestamp'].iloc[0])[:16].replace('T', ' ')
            st.write(f"**Last Sync (UTC):** {latest_ts}")
        except:
            st.write("**Last Sync (UTC):** N/A")

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
    # Show the interactive table [Streamlit Dataframe](https://docs.streamlit.io)
    st.dataframe(df, use_container_width=True)
else:
    st.warning("Dashboard is currently empty. Check if the API is returning data or if your Athena table is empty.")
    st.info("Tip: Check your [AWS CloudWatch Logs](https://console.aws.amazon.com) for the weather_api_proxy Lambda to see the latest fetch status.")
