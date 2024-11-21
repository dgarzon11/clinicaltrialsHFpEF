#Streamlit app dashboard for MDM that visualizws data from duchenne_studies_sample.csv

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio  # Import the plotly.io module
from plotly.subplots import make_subplots        # Import the make_subplots function
import plotly.graph_objects as go                            # Import the plotly.graph_objects module       
pio.renderers.default = "browser"  # Set the default renderer to "browser"                                                                     

df = pd.read_csv("data/dmd_current.csv")
dfh = pd.read_csv("data/dmd_history.csv")

st.title("DMD Clinical Trials Dashboard")

st.markdown("## Latest Study Timestamp")
max_timestamp = pd.to_datetime(df['Timestamp'].max()).strftime('%d %B %Y %H:%M')
st.info(f"Last update: {max_timestamp}")



total_studies = df['NCTId'].nunique()
st.metric("Total number of studies", total_studies)

df_sorted = df.sort_values(by='LastUpdatePostDate', ascending=False)
df_sorted = df_sorted[['NCTId', 'LastUpdatePostDate', 'LeadSponsorName'] + [col for col in df_sorted.columns if col not in ['NCTId', 'LastUpdatePostDate', 'LeadSponsorName']]]
st.dataframe(df_sorted)

st.markdown("## Studies by Lead Sponsor")
top_sponsors = df['LeadSponsorName'].value_counts().nlargest(5)
fig = px.bar(top_sponsors.sort_values(ascending=False), y=top_sponsors.index, x=top_sponsors.values, orientation='h', color_discrete_sequence=["blue"])
fig.update_layout(xaxis_visible=False, annotations=[dict(x=xi, y=yi, text=str(xi), font_size=12, showarrow=False) for xi, yi in zip(top_sponsors.values, top_sponsors.index)])
st.plotly_chart(fig, use_container_width=True)

