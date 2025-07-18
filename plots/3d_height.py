import psycopg2
import pandas as pd
import plotly.graph_objects as go

DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'henritruetsch',
    'host': 'localhost',
    'port': 5432
}

def fetch_data():
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT 
            date_trunc('hour', timestamp) AS time_bin,
            x, y,
            COUNT(*) AS click_count
        FROM event
        GROUP BY time_bin, x, y
        ORDER BY time_bin;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def plot_3d_time_slider(df):
    frames = []
    grouped = df.groupby('time_bin')

    for time_bin, group in grouped:
        frame = go.Frame(
            data=[
                go.Scatter3d(
                    x=group['x'],
                    y=group['y'],
                    z=group['click_count'],
                    mode='markers',
                    marker=dict(
                        size=2,
                        color=group['click_count'],
                        colorscale='Viridis',
                        showscale=False
                    )
                )
            ],
            name=str(time_bin)
        )
        frames.append(frame)

    if not frames:
        print("No data to show.")
        return

    # Base figure with first frame
    fig = go.Figure(
        data=frames[0].data,
        layout=go.Layout(
            title="r/place Pixel Activity Over Time",
            scene=dict(
                xaxis=dict(title='X', range=[-1000, 1000], zeroline=True),
                yaxis=dict(title='Y', range=[-1000, 1000], zeroline=True),
                zaxis=dict(title='Clicks'),
            ),
            updatemenus=[dict(
                type='buttons',
                showactive=False,
                buttons=[dict(label='Play', method='animate', args=[None])]
            )],
            sliders=[{
                "active": 0,
                "steps": [
                    {
                        "label": str(frame.name),
                        "method": "animate",
                        "args": [[frame.name], {"mode": "immediate"}]
                    }
                    for frame in frames
                ]
            }]
        ),
        frames=frames
    )

    fig.show()

if __name__ == "__main__":
    df = fetch_data()
    plot_3d_time_slider(df)
