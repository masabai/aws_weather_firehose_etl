"""
Streamlit Dashboard: HHS Region 9 Health Watch
---------------------------------------------
Visualizes real-time health and weather metrics derived from the AWS
Athena 'weather_gold' table.

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
from pyathena import connect
import altair as alt

# 1. Page & AWS Configuration
st.set_page_config(page_title="HHS Region 9 Health Watch", layout="wide")

# Fixed Configuration
REGION = "us-west-2"
S3_STAGING = "s3://weather-stream-data-etl/athena-results/"
DB = "steam_weather"

def get_data():
    """
    Connects to Athena using credentials from Streamlit Secrets.
    """
    # Streamlit Cloud retrieves these from the 'Secrets' panel in your dashboard
    conn = connect(
        s3_staging_dir=S3_STAGING,
        region_name=REGION,
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
    )
    query = f"SELECT * FROM {DB}.weather_gold ORDER BY timestamp DESC"
    return pd.read_sql(query, conn)

# 2. Sidebar & Global Controls
st.title("Weather & Flu Watch Alerts")
st.sidebar.header("Pipeline Status: LIVE")

if st.sidebar.button('Refresh Data'):
    st.cache_data.clear()
    st.rerun()

# 3. Data Ingestion
df = get_data()

# 4. KPI Metric Row
col1, col2, col3 = st.columns(3)

with col1:
    high_risk_count = len(df[df['flu_risk_category'] == 'High Risk'])
    st.metric("High Risk Alerts", high_risk_count, delta_color="inverse")

with col2:
    avg_temp = round(df['temp_current_f'].mean(), 1)
    st.metric("Avg Region Temp", f"{avg_temp}°F")

with col3:
    if not df.empty:
        # Puts the most recent sync time in the header
        latest_update = df['timestamp'].iloc[0][:16].replace('T', ' ')
        st.write(f"**Last Sync (UTC):** {latest_update}")
    else:
        st.write("**Last Sync (UTC):** No Data Available")

# 5. Visualizations: Regional Risk Breakdown
st.subheader("Regional Risk Breakdown")

color_scale = alt.Scale(
    domain=['High Risk', 'Moderate Risk', 'Low Risk'],
    range=['#FF0000', '#0000FF', '#00FF00']
)

chart = alt.Chart(df).mark_bar().encode(
    x=alt.X('state:N', title='State'),
    y=alt.Y('cold_flu_index:Q', title='Cold/Flu Index'),
    color=alt.Color('flu_risk_category:N', scale=color_scale, title='Risk Level'),
    tooltip=['location', 'cold_flu_index', 'flu_risk_category']
).properties(height=400)

st.altair_chart(chart, use_container_width=True)

# 6. Raw Data Inspection
st.subheader("Raw Health Data")
st.dataframe(df, use_container_width=True)
