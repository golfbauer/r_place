import cv2
import os
from glob import glob
from natsort import natsorted

# -------------------------------
# CONFIGURATION
IMAGE_FOLDER = "output_minute_canvases"
OUTPUT_VIDEO = "canvas_timelapse.mp4"
FPS = 5  # Frames per second (adjust as needed)

# -------------------------------
# Load image paths and sort them
image_files = natsorted(glob(os.path.join(IMAGE_FOLDER, "canvas_*.png")))

if not image_files:
    raise ValueError("❌ No images found in the folder. Check the path or filenames.")

# -------------------------------
# Read first image to get frame size
first_frame = cv2.imread(image_files[0])
height, width, _ = first_frame.shape

# -------------------------------
# Initialize video writer
fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # or use 'XVID'
out = cv2.VideoWriter(OUTPUT_VIDEO, fourcc, FPS, (width, height))

print(f"🎬 Creating video ({len(image_files)} frames, {FPS} fps)...")

# -------------------------------
# Write each image to the video
for i, file in enumerate(image_files):
    frame = cv2.imread(file)
    if frame is None:
        print(f"⚠️ Skipping unreadable frame: {file}")
        continue
    out.write(frame)
    if i % 100 == 0 or i == len(image_files) - 1:
        print(f"[{i+1}/{len(image_files)}] Added: {os.path.basename(file)}")

# -------------------------------
# Finalize
out.release()
print(f"✅ Video saved as: {OUTPUT_VIDEO}")
