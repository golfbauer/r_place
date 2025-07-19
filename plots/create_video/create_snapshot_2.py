import os
import warnings
import pandas as pd
import numpy as np
from PIL import Image
from datetime import timedelta, datetime

from manage.connection import fetch
from settings import IMAGE_FOLDER

X_MIN, X_MAX = -1500, 1499
Y_MIN, Y_MAX = -1000, 999
WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1

def delete_folder(path: str):
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

def get_min_max_timestamps():
    df = fetch("SELECT MIN(timestamp) AS min_time, MAX(timestamp) AS max_time FROM event")
    return pd.to_datetime(df.loc[0, "min_time"]), pd.to_datetime(df.loc[0, "max_time"])

def fetch_minute_data(start: datetime, end: datetime):
    query = """
        SELECT DATE_TRUNC('minute', timestamp) AS minute,
               x, y, color, timestamp
        FROM event
        WHERE timestamp >= %s AND timestamp < %s
        ORDER BY timestamp;
    """
    return fetch(query, (start, end))

def render_image(canvas: np.ndarray, output_path: str):
    img = Image.fromarray(canvas, mode="RGB")
    img.save(output_path)

def prepopulate_canvas_and_map(canvas, pixel_map, resume_time):
    print(f"Preloading canvas snapshot until {resume_time}...")
    query = """
        SELECT DISTINCT ON (x, y) x, y, color, timestamp
        FROM event
        WHERE timestamp < %s
        ORDER BY x, y, timestamp DESC;
    """
    df = fetch(query, (resume_time,))
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    for row in df.itertuples():
        x_idx = row.x - X_MIN
        y_idx = row.y - Y_MIN
        if 0 <= x_idx < WIDTH and 0 <= y_idx < HEIGHT:
            try:
                rgb = tuple(int(row.color[i:i+2], 16) for i in (1, 3, 5))
                canvas[y_idx, x_idx] = rgb
                pixel_map[(row.x, row.y)] = row.color
            except:
                continue
    print(f"Preloaded {len(pixel_map):,} pixels onto canvas.")

def process_frames():
    os.makedirs(IMAGE_FOLDER, exist_ok=True)
    # delete_folder(IMAGE_FOLDER)

    start_time, end_time = get_min_max_timestamps()
    start_time = pd.to_datetime("2023-07-24 08:22:00", utc=True)
    current = start_time.replace(second=0, microsecond=0)
    step = timedelta(hours=1)

    total_expected_images = int((end_time - current).total_seconds() // 60)
    total_images = 0

    canvas = np.ones((HEIGHT, WIDTH, 3), dtype=np.uint8) * 255
    pixel_map = {}

    prepopulate_canvas_and_map(canvas, pixel_map, current)

    while current < end_time:
        next_hour = current + step
        df = fetch_minute_data(current, next_hour)
        df['minute'] = pd.to_datetime(df['minute'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        for minute in sorted(df['minute'].dropna().unique()):
            minute_df = df[df['minute'] == minute]
            minute_df = minute_df.sort_values('timestamp').drop_duplicates(['x', 'y'], keep='last')

            for row in minute_df.itertuples():
                key = (row.x, row.y)
                if pixel_map.get(key) != row.color:
                    x_idx = row.x - X_MIN
                    y_idx = row.y - Y_MIN
                    if 0 <= x_idx < WIDTH and 0 <= y_idx < HEIGHT:
                        try:
                            rgb = tuple(int(row.color[i:i+2], 16) for i in (1, 3, 5))
                            canvas[y_idx, x_idx] = rgb
                            pixel_map[key] = row.color
                        except:
                            continue

            filename = minute.strftime("canvas_%Y%m%d_%H%M.png")
            filepath = os.path.join(IMAGE_FOLDER, filename)
            render_image(canvas, filepath)

            total_images += 1
            print(f"[{total_images} / {total_expected_images}] Saved {filename}")

        current = next_hour

    print("Done")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
    process_frames()
