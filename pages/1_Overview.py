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

# -----------------------TRABAJANDO AQUI-----------------------------
# Sort Timestamp in descending order and drop duplicates to get unique dates
unique_dates = df_history['Timestamp'].drop_duplicates().sort_values(ascending=False)

# Extract the last two distinct dates
latest_date = unique_dates.iloc[0]
previous_date = unique_dates.iloc[1]

print(latest_date)
print(previous_date)
# ------------------------------------------------------------------------

col1.metric("Total Trials", total_trials, "+12%")
col2.metric("Recruiting", recruiting_trials, "+5%")
col3.metric("Completed", completed_trials, "+3%")

# Bar Chart for OverallStatus
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

# Clinical Trials Trend
st.markdown("### Clinical Trials Trend")
df_timeline = df_current.groupby(df_current['StartDate'].dt.month)['NCTId'].count().reset_index()
df_timeline.columns = ['Month', 'Trials']

fig = px.line(
    df_timeline,
    x='Month',
    y='Trials',
    markers=True,
    title="Clinical Trials Trend",
    labels={"Month": "Month", "Trials": "Number of Trials"},
)
fig.update_traces(line=dict(color="blue", width=2), marker=dict(size=8))
fig.update_layout(title_x=0.5)

st.plotly_chart(fig, use_container_width=True)

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

