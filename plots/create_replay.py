import psycopg2
import pandas as pd
import numpy as np
from PIL import Image
from io import BytesIO
import base64
from dash import Dash, dcc, html, Output, Input

from manage.connection import fetch

# Habbo
X_MIN, X_MAX = 1650, 1730
Y_MIN, Y_MAX = 930, 820

# Video
# X_MIN, X_MAX = 1660, 1704
# Y_MIN, Y_MAX = 827, 796

# Turkey
# X_MIN, X_MAX = 1128, 1340
# Y_MIN, Y_MAX = 710, 650


X_MIN = X_MIN - 1500
X_MAX = X_MAX - 1500
Y_MIN = (Y_MIN - 1000) * -1
Y_MAX = (Y_MAX - 1000) * -1
WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1

def load_data():
    print("Fetching pixel history...")
    query = f"""
        SELECT DISTINCT ON (hour_bucket, x, y)
            hour_bucket,
            x,
            y,
            color,
            timestamp
        FROM (
            SELECT
                DATE_TRUNC('minute', timestamp) AS hour_bucket,
                x,
                y,
                color,
                timestamp
            FROM event
            WHERE x >= {X_MIN} AND x <= {X_MAX} AND y >= {Y_MIN} AND y <= {Y_MAX}
        ) AS sub
        ORDER BY hour_bucket, x, y, timestamp DESC;
    """
    df = fetch(query)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"Loaded {len(df):,} pixel events.")
    return df

df_all = load_data()
unique_times = sorted(df_all['timestamp'].dt.floor('1min').unique())

def render_canvas_at(df: pd.DataFrame, timestamp: pd.Timestamp) -> str:
    df_filtered = df[df['timestamp'] <= timestamp]
    df_latest = df_filtered.sort_values('timestamp').drop_duplicates(subset=['x', 'y'], keep='last')

    canvas = np.ones((HEIGHT, WIDTH, 3), dtype=np.uint8) * 255  # white base

    for row in df_latest.itertuples():
        x_idx = row.x - X_MIN
        y_idx = row.y - Y_MIN
        try:
            rgb = tuple(int(row.color[i:i+2], 16) for i in (1, 3, 5))
            canvas[y_idx, x_idx] = rgb
        except:
            continue

    img = Image.fromarray(canvas, mode="RGB")
    buf = BytesIO()
    img.save(buf, format="PNG")
    encoded = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"

app = Dash(__name__)
app.title = "r/place Canvas Animation"

app.layout = html.Div([
    html.H2("🎞️ r/place Canvas Animation"),
    dcc.Interval(
        id='interval-component',
        interval=50,  # ms between frames (adjust as desired)
        n_intervals=0
    ),
    dcc.Store(id='frame-index', data=0),
    html.Div(id="timestamp-display", style={"margin": "10px 0"}),
    html.Img(id="canvas-image", style={"width": "100%", "maxWidth": "1200px", "border": "1px solid #ccc"})
])

@app.callback(
    Output('frame-index', 'data'),
    Input('interval-component', 'n_intervals'),
)
def update_index(n):
    return n % len(unique_times)

@app.callback(
    Output("canvas-image", "src"),
    Output("timestamp-display", "children"),
    Input("frame-index", "data")
)
def update_image(time_index):
    timestamp = unique_times[time_index]
    img_src = render_canvas_at(df_all, timestamp)
    return img_src, f"🕒 Canvas state at: {timestamp}"

if __name__ == "__main__":
    app.run(debug=True)