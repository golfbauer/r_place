import requests

from settings import CSV_DIR

FILE_URL = "https://placedata.reddit.com/data/canvas-history/2023/2023_place_canvas_history-{}.csv.gzip"

def download_csv():
    for f in range(53):
        file_index = str(f).zfill(12)
        url = FILE_URL.format(file_index)
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            print(f"File {file_index} not found at {url}. Skipping.")
            continue

        with open(f"{CSV_DIR}/2023_place_canvas_history-{file_index}.csv.gzip", 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded: 2023_place_canvas_history-{file_index}.csv.gzip")

if __name__ == "__main__":
    download_csv()