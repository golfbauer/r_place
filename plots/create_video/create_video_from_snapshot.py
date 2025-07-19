import random

import cv2
import os
from glob import glob
from natsort import natsorted

from settings import IMAGE_FOLDER, VIDEO_FOLDER

FPS = 20

os.makedirs(VIDEO_FOLDER, exist_ok=True)
image_files = natsorted(glob(os.path.join(IMAGE_FOLDER, "canvas_*.png")))

if not image_files:
    raise ValueError("No images found in the folder. Check the path or filenames.")

first_frame = cv2.imread(image_files[0])
height, width, _ = first_frame.shape

fourcc = cv2.VideoWriter_fourcc(*'mp4v')
file_name = f"canvas_timelapse_{random.randint(1000, 9999)}.mp4"
out = cv2.VideoWriter(f"{VIDEO_FOLDER}/{file_name}", fourcc, FPS, (width, height))

print(f"Creating video ({len(image_files)} frames, {FPS} fps)...")

for i, file in enumerate(image_files):
    frame = cv2.imread(file)
    if frame is None:
        print(f"Skipping unreadable frame: {file}")
        continue
    out.write(frame)
    if i % 100 == 0 or i == len(image_files) - 1:
        print(f"[{i+1}/{len(image_files)}] Added: {os.path.basename(file)}")

out.release()
print(f"Video saved to: {VIDEO_FOLDER} as {file_name}")
