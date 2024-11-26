import streamlit as st
import pandas as pd

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")
df_history = pd.read_csv("data/dmd_history.csv")

st.title("Download Reports")
st.markdown("### Export Data and Reports")

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
