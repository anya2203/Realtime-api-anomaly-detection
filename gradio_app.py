import gradio as gr
import pandas as pd
import random

def get_data():
    # Simulate real-time values (based on your producer behavior)
    data = {
        "IP": ["192.168.1.1", "10.0.0.2", "172.16.0.5", "ATTACKER"],
        "Requests": [
            random.randint(1, 5),
            random.randint(1, 5),
            random.randint(1, 5),
            random.randint(15, 30)
        ]
    }

    df = pd.DataFrame(data)

    alert = ""
    if df["Requests"].max() > 15:
        alert = "🚨 ALERT: Possible Bot Attack Detected!"

    return df, alert

with gr.Blocks() as app:
    gr.Markdown("# 🚨 API Attack Detection Dashboard")

    table = gr.Dataframe(label="Live API Traffic")
    alert_box = gr.Textbox(label="Alert")

    btn = gr.Button("Refresh Data")

    btn.click(fn=get_data, outputs=[table, alert_box])

app.launch()
