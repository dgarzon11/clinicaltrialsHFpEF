import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt

#ALL PAGES -----------------------------------------------------------------
from utils import load_data_and_update_sidebar

# Load data and update sidebar
load_data_and_update_sidebar()

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

# Page title
st.title("Overview")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# ------------------------------------------------------------------------

df_current['StartDate'] = pd.to_datetime(df_current['StartDate'], errors='coerce')

# Key Metrics
col1, col2, col3, col4 = st.columns(4)

total_trials = df_current['NCTId'].nunique()
recruiting_trials = df_current[df_current['OverallStatus'] == "RECRUITING"]['NCTId'].nunique()
completed_trials = df_current[df_current['OverallStatus'] == "COMPLETED"]['NCTId'].nunique()

# Sort Timestamp in descending order and drop duplicates to get unique dates
unique_dates = df_history['Timestamp'].drop_duplicates().sort_values(ascending=False)

# Extract the last two latest dates
latest_date = unique_dates.iloc[0]
previous_date = unique_dates.iloc[1]

latest_recruiting = df_history[df_history['Timestamp'] == latest_date][df_history['OverallStatus'] == "RECRUITING"]['NCTId'].nunique()
previous_recruiting = df_history[df_history['Timestamp'] == previous_date][df_history['OverallStatus'] == "RECRUITING"]['NCTId'].nunique()
recruiting_percentage_change = (latest_recruiting - previous_recruiting) / previous_recruiting * 100 if previous_recruiting > 0 else 0

latest_completed = df_history[df_history['Timestamp'] == latest_date][df_history['OverallStatus'] == "COMPLETED"]['NCTId'].nunique()
previous_completed = df_history[df_history['Timestamp'] == previous_date][df_history['OverallStatus'] == "COMPLETED"]['NCTId'].nunique()
completed_percentage_change = (latest_completed - previous_completed) / previous_completed * 100 if previous_completed > 0 else 0

latest_total = df_history[df_history['Timestamp'] == latest_date]['NCTId'].nunique()
previous_total = df_history[df_history['Timestamp'] == previous_date]['NCTId'].nunique()
percentage_change = (latest_total - previous_total) / previous_total * 100 if previous_total > 0 else 0

col1.metric("Total Trials", latest_total, f"{percentage_change:+.2f}%")
col2.metric("Recruiting", recruiting_trials, f"{recruiting_percentage_change:+.2f}%")
col3.metric("Completed", completed_trials, f"{completed_percentage_change:+.2f}%")

# Create 2 columns
col1, col2 = st.columns(2)

# Line Chart for OverallStatus Over Time
with col1:
    st.markdown("### Overall Status Over Time")
    trend_data = df_history.groupby(['Timestamp', 'OverallStatus'])['NCTId'].count().reset_index()
    trend_data = trend_data.pivot(index='Timestamp', columns='OverallStatus', values='NCTId').fillna(0)
    trend_data['Total Trials'] = trend_data.sum(axis=1)

    fig_line = px.line(
        trend_data,
        x=trend_data.index,
        y=['Total Trials', 'RECRUITING', 'COMPLETED'],
        title="Total Trials, Recruiting, and Completed Trials Over Time",
        labels={'value': 'Number of Trials', 'Timestamp': 'Date'}
    )
    st.plotly_chart(fig_line, use_container_width=True)


# Bar Chart for OverallStatus
with col2:
    st.markdown("### Overall Status Distribution")
    status_distribution = df_current.groupby('OverallStatus')['NCTId'].nunique().reset_index()
    status_distribution.columns = ['OverallStatus', 'UniqueTrials']

    status_distribution = status_distribution.sort_values(by='UniqueTrials', ascending=True)

    fig_bar = px.bar(
        status_distribution,
        y='OverallStatus',
        x='UniqueTrials',
        title="Unique Trials by Overall Status",
        labels={"OverallStatus": "Overall Status", "UniqueTrials": "Number of Unique Trials"},
        text='UniqueTrials',
        orientation='h'
    )
    fig_bar.update_traces(marker_color='orange')
    fig_bar.update_layout(title_x=0.5)

    st.plotly_chart(fig_bar, use_container_width=True)

# Recent Trials Section
st.markdown("### Recent Trials")
recent_trials = df_current.sort_values(by='StartDate', ascending=False).head(3)

for _, trial in recent_trials.iterrows():
    st.write(
        f"**{trial['BriefTitle']}**  \n"
        f"**NCT ID**: {trial['NCTId']}  \n"
        f"**Phase**: {trial.get('Phase', 'N/A')}  \n"
        f"**Status**: {trial['OverallStatus']}"
    )
    if trial['OverallStatus'] == "RECRUITING":
        st.success("Recruiting")
    elif trial['OverallStatus'] == "COMPLETED":
        st.info("Completed")
    else:
        st.warning(trial['OverallStatus'])
    st.divider()

# Add "View All" Button (Optional for Navigation)
if st.button("View All Trials"):
    st.write("Navigate to the Study Details page.")

