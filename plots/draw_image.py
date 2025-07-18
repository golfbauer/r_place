import psycopg2
import pandas as pd
import numpy as np
from PIL import Image
from tqdm import tqdm

DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'henritruetsch',
    'host': 'localhost',
    'port': 5432
}

# Canvas bounds (centered at 0,0)
X_MIN, X_MAX = -1500, 1499
Y_MIN, Y_MAX = -1000, 1000
WIDTH = X_MAX - X_MIN + 1
HEIGHT = Y_MAX - Y_MIN + 1

def fetch_canvas_state_at(timestamp: str):
    conn = psycopg2.connect(**DB_PARAMS)
    query = f"""
        SELECT DISTINCT ON (x, y)
            x, y, color
        FROM event
        WHERE timestamp <= %s
        ORDER BY x, y, timestamp DESC;
    """
    print(f"⏳ Querying canvas state at {timestamp} ...")
    df = pd.read_sql(query, conn, params=[timestamp])
    conn.close()
    return df

def draw_canvas(df, output_file=None):
    print(f"🎨 Drawing canvas with {len(df)} pixels...")

    # Initialize canvas (white background)
    canvas = np.ones((HEIGHT, WIDTH, 3), dtype=np.uint8) * 255

    for row in tqdm(df.itertuples(), total=len(df)):
        x_idx = row.x - X_MIN
        y_idx = (row.y * -1) - Y_MIN

        hex_color = row.color.lstrip("#")
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

        canvas[y_idx, x_idx] = rgb  # y = row, x = column

    img = Image.fromarray(canvas, mode="RGB")

    if output_file:
        img.save(output_file)
        print(f"📸 Saved canvas to: {output_file}")

    img.show()

if __name__ == "__main__":
    # Target time to reconstruct
    target_timestamp = "2023-07-21 12:00:00"

    df = fetch_canvas_state_at(target_timestamp)
    draw_canvas(df, output_file="canvas_at_2023-07-21_1200.png")
