import streamlit as st
import pandas as pd

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")
df_history = pd.read_csv("data/dmd_history.csv")

st.title("Study Details")
st.markdown("### Explore detailed information about current and historical studies")

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
