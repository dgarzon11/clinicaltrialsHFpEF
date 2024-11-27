# utils.py
import streamlit as st
import pandas as pd


def load_data_and_update_sidebar():
    """Set the Streamlit page configuration."""
    st.set_page_config(
        page_icon="📊",
        layout="wide",
    )
    
    """Load datasets into session state and update the sidebar with the latest study update timestamp."""
    # Load data into session state if not already loaded
    if "df_current" not in st.session_state:
        st.session_state.df_current = pd.read_csv("data/dmd_current.csv")
    if "df_history" not in st.session_state:
        st.session_state.df_history = pd.read_csv("data/dmd_history.csv")

    # Access data from session state
    df_current = st.session_state.df_current

    # Ensure 'Timestamp' column is in datetime format
    df_current['Timestamp'] = pd.to_datetime(df_current['Timestamp'], errors='coerce')

    # Calculate the latest timestamp
    latest_timestamp = df_current['Timestamp'].max().strftime('%d %B %Y %H:%M')

    # Update the sidebar with the latest study update
    with st.sidebar:
        st.markdown("### Latest Data Extraction Date")
        st.info(f"{latest_timestamp}")
        
        

