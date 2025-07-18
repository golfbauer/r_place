import requests

FILE_URL = "https://placedata.reddit.com/data/canvas-history/2023/2023_place_canvas_history-{}.csv.gzip"

def download_csv():
    for f in range(53):
        file_index = str(f).zfill(12)
        url = FILE_URL.format(file_index)
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(f"/Users/henritruetsch/PycharmProjects/r_place/csv_files/2023_place_canvas_history-{file_index}.csv.gzip", 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: 2023_place_canvas_history-{file_index}.csv.gzip")
        else:
            print(f"Failed to download: {url} (Status code: {response.status_code})")

input_dir = "/Users/henritruetsch/PycharmProjects/r_place/csv_files"

if __name__ == "__main__":
    download_csv()