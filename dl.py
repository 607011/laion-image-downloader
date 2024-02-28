#!/usr/bin/env python3

from PIL import Image
from multiprocessing.pool import ThreadPool
from threading import BoundedSemaphore
import pyarrow.parquet as pq
import pyarrow.compute as pc
from urllib.parse import urlparse
import glob
import hashlib
import io
import os
import time
from downloadhelper import download_image_with_retry


def main():
    PROCESSED_FILE = "processed.txt"
    COLS = ["URL", "HEIGHT", "WIDTH", "TEXT"]
    ALLOWED_EXTS = [".jpg", ".png"]
    NUM_THREADS_DOWNLOAD = 80
    NUM_THREADS_CONVERSION = os.cpu_count()
    MIN_IMG_SIZE = 256
    DEFAULT_FILTER_REGEX = r"\bsheep\b"

    filter_regex = DEFAULT_FILTER_REGEX

    # PyArrow filter to select only images that are square
    # and have `SIZE` pixels in each dimension
    ONLY_SQUARE = (
        (pc.field("WIDTH") == pc.field("HEIGHT"))
        & (pc.field("WIDTH") >= pc.scalar(MIN_IMG_SIZE))
        & pc.match_substring_regex(pc.field("TEXT"), filter_regex, ignore_case=True)
    )

    filter_rules = ONLY_SQUARE

    files = set(glob.glob("data/the-eye.eu/*.parquet"))
    if os.path.isfile(PROCESSED_FILE):
        processed = {file.strip() for file in open(PROCESSED_FILE, "r").readlines()}
        files = files.difference(processed)
    if len(files) == 0:
        print("Nothing to do.")
        return

    num_threads_download = NUM_THREADS_DOWNLOAD
    num_threads_conversion = NUM_THREADS_CONVERSION
    disallowed_header_directives = []
    size = (MIN_IMG_SIZE, MIN_IMG_SIZE)

    def entry_generator(file: str):
        table = pq.read_table(file, columns=COLS, filters=filter_rules)
        print(f"""Applying filter {filter_rules}""")
        print(f"""Found {table.num_rows} entries.""")
        for batch in table.to_batches():
            for _idx, row in batch.to_pandas().iterrows():
                sem.acquire()
                url, _width, _height, _text = row.values
                yield url

    def download_task(url):
        url, img_bytes, err_msg = download_image_with_retry(
            url,
            timeout=10,
            retries=0,
            disallowed_header_directives=disallowed_header_directives,
        )
        return url, img_bytes, err_msg

    def save_image_task(url, img_bytes):
        _filename, file_ext = os.path.splitext(urlparse(url).path)
        if file_ext not in ALLOWED_EXTS:
            return
        hash = hashlib.sha256(img_bytes).hexdigest()
        CHUNK_SIZE = 8
        path = os.path.join("images", *[hash[i : i + CHUNK_SIZE] for i in range(0, 64 - CHUNK_SIZE, CHUNK_SIZE)])
        os.makedirs(path, exist_ok=True)
        img_filename = os.path.join(path, f"""{hash}{file_ext}""")
        if os.path.exists(img_filename):
            return
        img = Image.open(io.BytesIO(img_bytes))
        w, h = img.size
        assert w == h and w >= MIN_IMG_SIZE
        img = img.resize(size, resample=Image.BICUBIC)
        img.save(img_filename)

    sem = BoundedSemaphore(num_threads_download)
    conversion_pool = ThreadPool(num_threads_conversion)
    download_pool = ThreadPool(num_threads_download)
    t0 = time.time()
    file_counter = 0

    for file in files:
        print(f"""Processing {file} ...""")
        entries = download_pool.imap_unordered(download_task, entry_generator(file))
        for url, img_bytes, _err_msg in entries:
            file_counter += 1
            if img_bytes is not None:
                conversion_pool.apply_async(save_image_task, args=(url, img_bytes))
                print(
                    f"""\r\u001b[32m{file_counter / (time.time() - t0):.2f}/s\u001b[0m""",
                    end="",
                )
            sem.release()
        print("\r\u001b[K")
        with open(PROCESSED_FILE, "a+") as ready_file:
            print(file, file=ready_file)

    conversion_pool.close()
    conversion_pool.join()
    download_pool.terminate()
    download_pool.join()
    print(
        f"""READY.

total time:     {time.time() - t0:.1f} secs
total files:    {file_counter}
avg. files/sec: {file_counter / (time.time() - t0):.1f}
"""
    )


if __name__ == "__main__":
    main()
