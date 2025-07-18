import psycopg2
import pandas as pd
import numpy as np
import plotly.express as px

DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': 5432
}

def fetch_pixel_counts():
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT x, y, COUNT(*) as click_count
        FROM event
        GROUP BY x, y;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def plot_heatmap(df):
    # Define known canvas bounds
    x_min, x_max = -1500, 1499
    y_min, y_max = -1000, 1000
    width = x_max - x_min + 1  # = 3000
    height = y_max - y_min + 1  # = 2001

    heatmap = np.zeros((height, width), dtype=int)

    for row in df.itertuples():
        x = row.x - x_min  # shift to 0-based index
        y = (row.y * -1) - y_min
        heat = min(int(row.click_count), 20000)  # cap max heat
        heatmap[y, x] = heat  # row = y, column = x

    fig = px.imshow(
        heatmap,
        origin='lower',
        labels=dict(x="X", y="Y", color="Clicks"),
        color_continuous_scale='hot',
        zmax=100,  # enforce cap for colormap scaling
        title='r/place Total Click Heatmap (Capped at 20K)'
    )

    fig.update_layout(
        xaxis=dict(title="X", tickmode="linear", dtick=250),
        yaxis=dict(title="Y", tickmode="linear", dtick=250),
        width=1200,
        height=800
    )

    fig.show()

if __name__ == "__main__":
    df = fetch_pixel_counts()
    plot_heatmap(df)
