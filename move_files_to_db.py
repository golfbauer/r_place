import os
import gzip
from concurrent.futures import ProcessPoolExecutor

import psycopg2
import csv

from psycopg2.extras import execute_values

CSV_DIR = "/Users/henritruetsch/PycharmProjects/r_place/csv_files"
BATCH_SIZE = 10000
DB_PARAMS = {
    'dbname': 'r_place',
    'user': 'henritruetsch',
    'host': 'localhost',
    'port': 5432
}

def process_file(file):
    """Process a single file and insert rows into the event table."""
    file_path = os.path.join(CSV_DIR, file)
    print(f"Processing {file}")

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    event_rows = []

    with gzip.open(file_path, 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                timestamp = row['timestamp'].replace(' UTC', '')
                user = row['user']
                coord = row['coordinate']
                color = row['pixel_color']

                if ',' in coord and coord.count(',') == 1:
                    x_str, y_str = coord.split(',')
                    event_rows.append((timestamp, user, int(x_str), int(y_str), color))
                elif ',' in coord and coord.count(',') == 3:
                    x1, y1, x2, y2 = map(int, coord.split(','))
                    for i in range(x1, x2 + 1):
                        for j in range(y1, y2 + 1):
                            event_rows.append((timestamp, user, int(i), int(j), color))
            except Exception as e:
                print(f"Skipped row in {file}: {e}")
                continue

    if event_rows:
        for i in range(0, len(event_rows), BATCH_SIZE):
            batch = event_rows[i:i + BATCH_SIZE]
            execute_values(
                cur,
                "INSERT INTO event (timestamp, user_id, x, y, color) VALUES %s",
                batch,
                page_size=BATCH_SIZE
            )
            conn.commit()

    print(f"Inserted {len(event_rows)} events from {file}")

    cur.close()
    conn.close()

def main():
    files = [f for f in sorted(os.listdir(CSV_DIR)) if f.endswith(".csv.gzip")]

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(process_file, files)

    print("All files processed in parallel.")

if __name__ == "__main__":
    main()