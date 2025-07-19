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
WIDTH = X_MAX - X_MIN + 1  # 3000
HEIGHT = Y_MAX - Y_MIN + 1  # 2000

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

def render_image(pixel_map: dict, output_path: str):
    canvas = np.ones((HEIGHT, WIDTH, 3), dtype=np.uint8) * 255
    for (x, y), color in pixel_map.items():
        x_idx = x - X_MIN
        y_idx = y - Y_MIN
        if 0 <= x_idx < WIDTH and 0 <= y_idx < HEIGHT:
            try:
                rgb = tuple(int(color[i:i+2], 16) for i in (1, 3, 5))
                canvas[y_idx, x_idx] = rgb
            except:
                continue
    img = Image.fromarray(canvas, mode="RGB")
    img.save(output_path)

def process_frames():
    os.makedirs(IMAGE_FOLDER, exist_ok=True)
    delete_folder(IMAGE_FOLDER)

    start_time, end_time = get_min_max_timestamps()
    current = start_time.replace(second=0, microsecond=0)
    step = timedelta(hours=1)

    # Calculate total number of minutes to be processed
    total_expected_images = int((end_time - current).total_seconds() // 60)

    pixel_map = {}
    total_images = 0

    while current < end_time:
        next_hour = current + step
        df = fetch_minute_data(current, next_hour)
        df['minute'] = pd.to_datetime(df['minute'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        for minute in sorted(df['minute'].dropna().unique()):
            minute_df = df[df['minute'] == minute]
            minute_df = minute_df.sort_values('timestamp').drop_duplicates(['x', 'y'], keep='last')

            for row in minute_df.itertuples():
                pixel_map[(row.x, row.y)] = row.color

            filename = minute.strftime("canvas_%Y%m%d_%H%M.png")
            filepath = os.path.join(IMAGE_FOLDER, filename)
            render_image(pixel_map, filepath)
            total_images += 1
            print(f"[{total_images} / {total_expected_images}] Saved {filename}")

        current = next_hour

    print("Done")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
    process_frames()
