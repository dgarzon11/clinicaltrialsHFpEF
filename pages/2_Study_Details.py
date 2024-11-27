import streamlit as st
import pandas as pd

#ALL PAGES -----------------------------------------------------------------
from utils import load_data_and_update_sidebar

# Load data and update sidebar
load_data_and_update_sidebar()

# Access data
df_current = st.session_state.df_current
df_history = st.session_state.df_history

# Page title
st.title("Study Details")
st.markdown("Monitor and analyze clinical trials data in real-time.")

# ------------------------------------------------------------------------

# Table with filters
status_filter = st.selectbox("Filter by Status", options=df_current['OverallStatus'].unique(), index=0)
filtered_data = df_current[df_current['OverallStatus'] == status_filter]
st.dataframe(filtered_data[['NCTId', 'BriefTitle', 'LeadSponsorName', 'OverallStatus', 'StartDate']])

# Download option
st.download_button(
    label="Download Filtered Data",
    data=filtered_data.to_csv(index=False).encode('utf-8'),
    file_name="filtered_study_details.csv",
    mime="text/csv",
)
