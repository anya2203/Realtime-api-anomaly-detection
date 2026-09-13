import streamlit as st
import pandas as pd
import os
import time

st.title("🚨 API Attack Detection Dashboard")

data_path = "/tmp/output"

while True:
    try:
        files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

        if files:
            latest = sorted(files)[-1]
            df = pd.read_csv(f"{data_path}/{latest}", header=None)
            df.columns = ["IP", "Requests"]

            st.subheader("Live API Traffic")
            st.table(df)

            if df["Requests"].max() > 15:
                st.error("🚨 ALERT: Bot Attack Detected!")

        else:
            st.write("Waiting for data...")

    except:
        st.write("Loading...")

    time.sleep(2)
