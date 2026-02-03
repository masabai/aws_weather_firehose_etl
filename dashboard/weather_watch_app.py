import streamlit as st
import pandas as pd
import requests
import altair as alt

# 1. Page Configuration
st.set_page_config(page_title="HHS Region 9 Health Watch", layout="wide")

# 2. API CONFIGURATION
API_URL = "https://3l6spjxqv5.execute-api.us-west-2.amazonaws.com/default/weather_api_proxy"


@st.cache_data(ttl=60)  # Cache data for 60 seconds to stay fresh
def get_data_from_api():
    """Fetches deduplicated Gold data from the Secure API Proxy."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.error(f"API Error: {e}")
        return pd.DataFrame()


# 3. Sidebar & Global Controls
st.title(" Weather & Flu Watch Alerts")
st.sidebar.header("Pipeline Status: LIVE")

if st.sidebar.button('Manual Refresh'):
    st.cache_data.clear()
    st.rerun()

# 4. Load Data
df = get_data_from_api()

if not df.empty:
    # 5. KPI Metric Row
    col1, col2, col3 = st.columns(3)
    with col1:
        high_risk_count = len(df[df['flu_risk_category'] == 'High Risk'])
        st.metric("High Risk Alerts", high_risk_count)

    with col2:
        # Athena/JSON might return numbers as strings; convert for math
        df['temp_current_f'] = pd.to_numeric(df['temp_current_f'])
        avg_temp = round(df['temp_current_f'].mean(), 1)
        st.metric("Avg Region Temp", f"{avg_temp}°F")

    with col3:
        latest_ts = df['timestamp'].iloc[0][:16].replace('T', ' ')
        st.write(f"**Last Sync (UTC):** {latest_ts}")

    # 6. Visualization
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

    st.altair_chart(chart, use_container_width=True)

    st.subheader("Raw Health Data")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("Waiting for fresh data from API...")
