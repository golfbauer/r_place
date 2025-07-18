import psycopg2
import pandas as pd
import numpy as np
from PIL import Image
import os
from datetime import timedelta

# -------------------------------
# CONFIGURATION
DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': 5432
}
# Habbo
# X_MIN, X_MAX = 1650, 1730
# Y_MIN, Y_MAX = 930, 820

# Video
X_MIN, X_MAX = 1660, 1704
Y_MIN, Y_MAX = 827, 796


# Turkey
# X_MIN, X_MAX = 1128, 1340
# Y_MIN, Y_MAX = 710, 650


X_MIN = X_MIN - 1500
X_MAX = X_MAX - 1500
Y_MIN = (Y_MIN - 1000) * -1
Y_MAX = (Y_MAX - 1000) * -1

# EVERYTHING
X_MIN, X_MAX = -1500, 1499
Y_MIN, Y_MAX = -1000, 999

WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1
OUTPUT_FOLDER = "output_minute_canvases"  # Folder to store images
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------------
# LOAD DATA
def load_data():
    print("⏳ Fetching pixel history by minute...")
    conn = psycopg2.connect(**DB_PARAMS)
    query = f"""
        SELECT DISTINCT ON (minute_bucket, x, y)
            minute_bucket,
            x,
            y,
            color,
            timestamp
        FROM (
            SELECT
                DATE_TRUNC('hour', timestamp) AS minute_bucket,
                x,
                y,
                color,
                timestamp
            FROM event
--             WHERE x >= {X_MIN} AND x <= {X_MAX} AND y >= {Y_MIN} AND y <= {Y_MAX}
        ) AS sub
        ORDER BY minute_bucket, x, y, timestamp DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"✅ Loaded {len(df):,} events")
    return df

# -------------------------------
# RENDER CANVAS TO IMAGE FILE
def render_canvas(df: pd.DataFrame, timestamp: pd.Timestamp, output_path: str):
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
    img.save(output_path)

# -------------------------------
# MAIN LOOP
def main():
    df_all = load_data()
    unique_minutes = sorted(df_all['timestamp'].dt.floor('h').unique())

    print(f"🧮 Generating {len(unique_minutes)} images...")
    for i, ts in enumerate(unique_minutes):
        filename = ts.strftime("canvas_%Y%m%d_%H%M.png")
        filepath = os.path.join(OUTPUT_FOLDER, filename)
        if not os.path.exists(filepath):
            render_canvas(df_all, ts, filepath)
            print(f"[{i+1}/{len(unique_minutes)}] ✅ Saved {filename}")
        else:
            print(f"[{i+1}/{len(unique_minutes)}] ⏩ Skipped (already exists): {filename}")

    print("🎉 Done.")

if __name__ == "__main__":
    main()