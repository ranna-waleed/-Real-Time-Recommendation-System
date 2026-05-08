import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import os
from datetime import datetime

# initialize dash app
app = dash.Dash(__name__)
app.title = "Real-Time Movie Recommendation Dashboard"

# paths to streaming output files
trending_path = "/home/hduser/streaming_output/trending"
user_activity_path = "/home/hduser/streaming_output/user_activity"
alerts_log_path = "/home/hduser/alerts_log/alerts.json"

def read_json_files(path):
    all_data = []
    if not os.path.exists(path):
        return pd.DataFrame()
    for file in os.listdir(path):
        if file.endswith(".json"):
            file_path = os.path.join(path, file)
            try:
                df = pd.read_json(file_path, lines=True)
                all_data.append(df)
            except Exception:
                continue
    if len(all_data) == 0:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def read_alerts():
    alerts = []
    if not os.path.exists(alerts_log_path):
        return alerts
    try:
        with open(alerts_log_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    alerts.append(json.loads(line))
    except Exception:
        pass
    return alerts[-20:]

# dashboard layout
app.layout = html.Div([

    html.H1(
        "Real-Time Movie Recommendation System Dashboard",
        style={"textAlign": "center", "marginBottom": "20px", "color": "#2c3e50"}
    ),

    html.P(
        "Dashboard auto-refreshes every 3 seconds",
        style={"textAlign": "center", "color": "grey"}
    ),

    # interval component for auto refresh
    dcc.Interval(
        id="interval-refresh",
        interval=3000,
        n_intervals=0
    ),

    # row 1 - trending items and user activity
    html.Div([

        # panel 1 - trending items bar chart
        html.Div([
            html.H3("Trending Items", style={"textAlign": "center"}),
            dcc.Graph(id="trending-chart")
        ], style={
            "width": "48%",
            "display": "inline-block",
            "verticalAlign": "top",
            "backgroundColor": "#f9f9f9",
            "padding": "15px",
            "borderRadius": "8px",
            "marginRight": "2%"
        }),

        # panel 2 - user activity chart
        html.Div([
            html.H3("User Activity", style={"textAlign": "center"}),
            dcc.Graph(id="user-activity-chart")
        ], style={
            "width": "48%",
            "display": "inline-block",
            "verticalAlign": "top",
            "backgroundColor": "#f9f9f9",
            "padding": "15px",
            "borderRadius": "8px"
        })

    ], style={"marginBottom": "20px"}),

    # row 2 - streaming metrics and alerts
    html.Div([

        # panel 3 - streaming metrics
        html.Div([
            html.H3("Streaming Metrics", style={"textAlign": "center"}),
            html.Div(id="streaming-metrics")
        ], style={
            "width": "48%",
            "display": "inline-block",
            "verticalAlign": "top",
            "backgroundColor": "#f9f9f9",
            "padding": "15px",
            "borderRadius": "8px",
            "marginRight": "2%"
        }),

        # panel 4 - alerts feed
        html.Div([
            html.H3("Live Alerts", style={"textAlign": "center"}),
            html.Div(id="alerts-feed")
        ], style={
            "width": "48%",
            "display": "inline-block",
            "verticalAlign": "top",
            "backgroundColor": "#f9f9f9",
            "padding": "15px",
            "borderRadius": "8px"
        })

    ], style={"marginBottom": "20px"}),

    # row 3 - recommendations panel
    html.Div([
        html.H3("Top Recommendations", style={"textAlign": "center"}),
        html.Div(id="recommendations-panel")
    ], style={
        "backgroundColor": "#f9f9f9",
        "padding": "15px",
        "borderRadius": "8px",
        "marginBottom": "20px"
    })

], style={"padding": "20px", "fontFamily": "Arial, sans-serif"})


# callback to update trending chart
@app.callback(
    Output("trending-chart", "figure"),
    Input("interval-refresh", "n_intervals")
)
def update_trending_chart(n):
    df = read_json_files(trending_path)
    if df.empty or "item_id" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="No data available yet")
        return fig
    if "trending_score" not in df.columns:
        df["trending_score"] = df.get("interaction_count", 1) * df.get("avg_rating", 0)
    top_items = df.nlargest(10, "trending_score")
    fig = px.bar(
        top_items,
        x="item_id",
        y="trending_score",
        color="avg_rating",
        labels={"item_id": "Movie ID", "trending_score": "Trending Score"},
        title="Top 10 Trending Movies"
    )
    return fig


# callback to update user activity chart
@app.callback(
    Output("user-activity-chart", "figure"),
    Input("interval-refresh", "n_intervals")
)
def update_user_activity(n):
    df = read_json_files(user_activity_path)
    if df.empty or "user_id" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="No data available yet")
        return fig
    top_users = df.nlargest(10, "interactions_count")
    fig = px.bar(
        top_users,
        x="user_id",
        y="interactions_count",
        color="avg_rating",
        labels={"user_id": "User ID", "interactions_count": "Number of Interactions"},
        title="Top 10 Most Active Users"
    )
    return fig


# callback to update streaming metrics
@app.callback(
    Output("streaming-metrics", "children"),
    Input("interval-refresh", "n_intervals")
)
def update_streaming_metrics(n):
    trending_df = read_json_files(trending_path)
    user_df = read_json_files(user_activity_path)

    total_interactions = 0
    total_items = 0
    total_users = 0
    avg_rating = 0

    if not trending_df.empty:
        total_items = trending_df["item_id"].nunique() if "item_id" in trending_df.columns else 0
        if "avg_rating" in trending_df.columns:
            avg_rating = round(trending_df["avg_rating"].mean(), 2)
        if "interaction_count" in trending_df.columns:
            total_interactions = int(trending_df["interaction_count"].sum())

    if not user_df.empty:
        total_users = user_df["user_id"].nunique() if "user_id" in user_df.columns else 0

    return html.Div([
        html.P("Total Interactions Processed: " + str(total_interactions)),
        html.P("Unique Items Seen: " + str(total_items)),
        html.P("Unique Users Active: " + str(total_users)),
        html.P("Average Rating: " + str(avg_rating)),
        html.P("Last Updated: " + datetime.now().strftime("%H:%M:%S"))
    ])


# callback to update alerts feed
@app.callback(
    Output("alerts-feed", "children"),
    Input("interval-refresh", "n_intervals")
)
def update_alerts(n):
    alerts = read_alerts()
    if not alerts:
        return html.P("No alerts triggered yet")
    alert_items = []
    for alert in reversed(alerts):
        color = "#e74c3c" if alert.get("alert_type") == "TRENDING_ITEM" else "#e67e22"
        alert_items.append(
            html.Div([
                html.Strong("[" + alert.get("alert_type", "") + "] "),
                html.Span(alert.get("message", "")),
                html.Br(),
                html.Small(alert.get("timestamp", ""), style={"color": "grey"})
            ], style={
                "borderLeft": "4px solid " + color,
                "paddingLeft": "10px",
                "marginBottom": "10px"
            })
        )
    return alert_items


# callback to update recommendations panel
@app.callback(
    Output("recommendations-panel", "children"),
    Input("interval-refresh", "n_intervals")
)
def update_recommendations(n):
    df = read_json_files(trending_path)
    if df.empty or "item_id" not in df.columns:
        return html.P("No recommendations available yet")
    if "trending_score" not in df.columns:
        df["trending_score"] = df.get("interaction_count", 1) * df.get("avg_rating", 0)
    top_5 = df.nlargest(5, "trending_score")[["item_id", "avg_rating", "trending_score"]]
    rows = []
    for i, row in top_5.iterrows():
        rows.append(
            html.Div([
                html.Span(
                    str(int(row["item_id"])),
                    style={"fontWeight": "bold", "marginRight": "10px"}
                ),
                html.Span("Avg Rating: " + str(round(row["avg_rating"], 2))),
                html.Span(
                    " | Trending Score: " + str(round(row["trending_score"], 2)),
                    style={"color": "grey", "marginLeft": "10px"}
                )
            ], style={
                "padding": "8px",
                "marginBottom": "5px",
                "backgroundColor": "white",
                "borderRadius": "5px"
            })
        )
    return rows


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
