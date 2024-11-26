import streamlit as st
import pandas as pd
import plotly.express as px

# Load datasets
df_current = pd.read_csv("data/dmd_current.csv")

st.title("Competitive Analysis")
st.markdown("### Compare Sponsors and Their Areas of Focus")

# Sponsors' study count
top_sponsors = df_current['LeadSponsorName'].value_counts().nlargest(10)
fig_sponsors = px.bar(
    top_sponsors,
    orientation='h',
    title="Top 10 Sponsors by Study Count",
)
st.plotly_chart(fig_sponsors, use_container_width=True)
