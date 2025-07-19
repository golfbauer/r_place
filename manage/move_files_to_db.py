import os
import gzip
from concurrent.futures import ProcessPoolExecutor

import psycopg2
import csv

from psycopg2.extras import execute_values

from settings import DATABASE, CSV_DIR

BATCH_SIZE = 10000

def process_file(file):
    file_path = os.path.join(CSV_DIR, file)
    print(f"Processing {file}")

    conn = psycopg2.connect(**DATABASE)
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
                if ',' not in coord:
                    continue
                comma_count = coord.count(',')
                if comma_count == 1:
                    x_str, y_str = coord.split(',')
                    event_rows.append((timestamp, user, int(x_str), int(y_str), color))
                elif comma_count == 2:
                    coord = coord.strip('{').strip('}').replace('X: ', '').replace('Y: ', '').replace('R: ', '')
                    x, y, r = coord.split(',')
                    x, y, r = (int(x), int(y), int(r))
                    r2 = r*r
                    for i in range(x - r, x + r + 1):
                        for j in range(y - r, y + r + 1):
                            if (i - x) ** 2 + (j - y) ** 2 <= r2:
                                event_rows.append((timestamp, user, int(x), int(y), color))
                elif comma_count == 3:
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