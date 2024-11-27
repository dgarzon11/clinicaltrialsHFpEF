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
st.title("Download Reports")
st.markdown("### Export datasets and visualizations")

# ------------------------------------------------------------------------

# Buttons for data download
st.download_button(
    label="Download Current Data",
    data=df_current.to_csv(index=False).encode('utf-8'),
    file_name="dmd_current.csv",
    mime="text/csv",
)
st.download_button(
    label="Download Historical Data",
    data=df_history.to_csv(index=False).encode('utf-8'),
    file_name="dmd_history.csv",
    mime="text/csv",
)
