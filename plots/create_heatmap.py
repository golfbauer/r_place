import numpy as np
import plotly.express as px

from manage.connection import fetch

QUERY =  """
    SELECT x, y, COUNT(*) as click_count
    FROM event
    GROUP BY x, y;
"""

def plot_heatmap():
    x_min, x_max = -1500, 1499
    y_min, y_max = -1000, 1000
    width = x_max - x_min + 1
    height = y_max - y_min + 1

    dataframe = fetch(QUERY)
    heatmap = np.zeros((height, width), dtype=int)
    for row in dataframe.itertuples():
        x = row.x - x_min
        y = (-1 * row.y) - y_min
        heatmap[y, x] = row.click_count

    fig = px.imshow(
        heatmap,
        origin='lower',
        labels=dict(x="X", y="Y", color="Clicks"),
        color_continuous_scale='hot',
        zmax=100,  # enforce cap for colormap scaling
        title='r/place Total Click Heatmap'
    )

    fig.update_layout(
        xaxis=dict(title="X", tickmode="linear", dtick=250),
        yaxis=dict(title="Y", tickmode="linear", dtick=250),
        width=1200,
        height=800
    )

    fig.show()

if __name__ == "__main__":
    plot_heatmap()
