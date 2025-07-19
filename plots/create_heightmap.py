import plotly.graph_objects as go
import numpy as np
from dash import Dash, dcc, html, Input, Output
from manage.connection import fetch

# TODO: THIS DOES NOT WORK

X_MIN, X_MAX = -1500, 1499
Y_MIN, Y_MAX = -1000, 999
WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1

def get_time_bins():
    df = fetch("""
        SELECT DISTINCT date_trunc('hour', timestamp) as time_bin
        FROM event
        ORDER BY time_bin
    """)
    return df['time_bin'].tolist()

def get_counts_for_bin(time_bin):
    return fetch("""
        SELECT x, y, COUNT(*) AS click_count
        FROM event
        WHERE date_trunc('hour', timestamp) = %s
        GROUP BY x, y
    """, (time_bin,))

def build_surface_figure(df, time_bin):
    z = np.zeros((HEIGHT, WIDTH), dtype=int)

    for row in df.itertuples():
        x = row.x - X_MIN
        y = row.y - Y_MIN
        if 0 <= x < WIDTH and 0 <= y < HEIGHT:
            z[y, x] = row.click_count

    fig = go.Figure(
        data=go.Surface(
            z=z,
            colorscale='Viridis',
            cmin=0,
            cmax=np.percentile(z, 99),
            showscale=True
        )
    )
    fig.update_layout(
        title=f"r/place 3D Activity Surface at {time_bin}",
        scene=dict(
            xaxis=dict(
                title='X',
                range=[0, WIDTH],
                tickvals=[0, WIDTH // 2, WIDTH - 1],
                ticktext=[X_MIN, 0, X_MAX]
            ),
            yaxis=dict(
                title='Y',
                range=[0, HEIGHT],
                tickvals=[0, HEIGHT // 2, HEIGHT - 1],
                ticktext=[Y_MIN, 0, Y_MAX]
            ),
            zaxis=dict(
                title='Clicks',
                range=[0, None]
            )
        ),
        margin=dict(l=0, r=0, b=0, t=40)
    )
    return fig

time_bins = get_time_bins()

app = Dash(__name__)
app.title = "r/place Surface Heightmap"

app.layout = html.Div([
    html.H2("r/place Surface Plot (Hourly)"),
    dcc.Slider(
        id='time-slider',
        min=0,
        max=len(time_bins) - 1,
        value=0,
        marks={i: str(t)[:13] for i, t in enumerate(time_bins[::max(1, len(time_bins)//10)])},
        tooltip={"placement": "bottom", "always_visible": True}
    ),
    html.Div(id='timestamp-display', style={"marginTop": "10px"}),
    dcc.Graph(id='surface-graph')
])

@app.callback(
    Output('surface-graph', 'figure'),
    Output('timestamp-display', 'children'),
    Input('time-slider', 'value')
)
def update_plot(index):
    time_bin = time_bins[index]
    df = get_counts_for_bin(time_bin)
    fig = build_surface_figure(df, time_bin)
    return fig, f"Showing pixel click distribution at: {time_bin}"

if __name__ == "__main__":
    app.run(debug=True)
