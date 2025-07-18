import psycopg2
import pandas as pd
import numpy as np
from PIL import Image
from io import BytesIO
import base64
from dash import Dash, dcc, html, Output, Input

# Constants
DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': 5432
}
X_MIN, X_MAX = -1500, 1499
Y_MIN, Y_MAX = -1000, 1000
WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1

# --------------------------------------------------
# Step 1: Load all data into memory (most recent per pixel per timestamp)
def load_data():
    print("⏳ Fetching pixel history...")
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT DISTINCT ON (hour_bucket, x, y)
    hour_bucket,
    x,
    y,
    color,
    timestamp
FROM (
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour_bucket,
        x,
        y,
        color,
        timestamp
    FROM event
) AS sub
ORDER BY hour_bucket, x, y, timestamp DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"✅ Loaded {len(df):,} pixel events.")
    return df

df_all = load_data()
unique_times = sorted(df_all['timestamp'].dt.floor('1h').unique())

# --------------------------------------------------
# Step 2: Function to generate canvas at a timestamp
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
            continue  # skip malformed colors

    img = Image.fromarray(canvas, mode="RGB")
    buf = BytesIO()
    img.save(buf, format="PNG")
    encoded = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"

# --------------------------------------------------
# Step 3: Create Dash app
app = Dash(__name__)
app.title = "r/place Canvas Time Explorer"

app.layout = html.Div([
    html.H2("🖼️ r/place Canvas Snapshot"),
    dcc.Slider(
        id="time-slider",
        min=0,
        max=len(unique_times) - 1,
        step=1,
        value=0,
        marks={i: str(t)[:13] for i, t in enumerate(unique_times[::max(1, len(unique_times)//10)])},
        tooltip={"placement": "bottom", "always_visible": True}
    ),
    html.Div(id="timestamp-display", style={"margin": "10px 0"}),
    html.Img(id="canvas-image", style={"width": "100%", "maxWidth": "1200px", "border": "1px solid #ccc"})
])

@app.callback(
    Output("canvas-image", "src"),
    Output("timestamp-display", "children"),
    Input("time-slider", "value")
)
def update_image(time_index):
    timestamp = unique_times[time_index]
    img_src = render_canvas_at(df_all, timestamp)
    return img_src, f"🕒 Canvas state at: {timestamp}"

# --------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)