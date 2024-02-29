#!/usr/bin/env python3

"""
LAION Image Downloader.

Copyright (c) 2024 Oliver Lau, oliver@ersatzworld.net

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from PIL import Image, UnidentifiedImageError
from multiprocessing.pool import ThreadPool
from threading import BoundedSemaphore
from itertools import permutations
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from urllib.parse import urlparse, quote
import hashlib
import io
import os
import time
from downloadhelper import download_image_with_retry
from writer import ParquetWriter
from argparse import ArgumentParser


def main():
    PROCESSED_FILE = "processed.txt"
    COLS = ["URL", "HEIGHT", "WIDTH", "TEXT"]
    ALLOWED_EXTS = ["jpg", "png"]
    NUM_THREADS_DOWNLOAD = 8 * os.cpu_count()
    MIN_IMG_SIZE = 128

    ap = ArgumentParser(
        prog="laion-dl",
        description="Downloader for images referenced by LAION parquet files",
    )
    ap.add_argument(
        "parquet",
        nargs="+",
        type=str,
    )
    ap.add_argument(
        "-O",
        "--output",
        type=str,
        default="images.parquet",
        help="Write resulting parquet file to OUTPUT"
    )
    ap.add_argument(
        "-N",
        "--num-processes",
        type=int,
        default=NUM_THREADS_DOWNLOAD,
        help=f"""Launch NUM_PROCESSES processes (default: {NUM_THREADS_DOWNLOAD})""",
    )
    ap.add_argument(
        "-K",
        "--keywords",
        type=str,
        default=None,
        help="comma separated list of keywords images must be tagged with",
    )
    ap.add_argument(
        "--min-img-size",
        type=int,
        default=MIN_IMG_SIZE,
        help="images should have a width and height of at least NUM_PIXELS",
        metavar="NUM_PIXELS",
    )
    ap.add_argument(
        "--no-continue",
        action="store_true",
        help="ignore files written to processed.txt, instead begin from scratch"
    )
    args = ap.parse_args()

    print("\u001b[1mLAION image downloader.\u001b[0m\n")

    min_img_size = args.min_img_size

    if args.no_continue:
        if os.path.isfile(PROCESSED_FILE):
            os.remove(PROCESSED_FILE)

    files = set(args.parquet)
    if os.path.isfile(PROCESSED_FILE):
        processed = {file.strip() for file in open(PROCESSED_FILE, "r").readlines()}
        files = files.difference(processed)
    if len(files) == 0:
        print("Nothing to do.")
        return

    filter_rules = (
        (pc.field("WIDTH") >= pc.scalar(min_img_size))
        &
        (pc.field("HEIGHT") >= pc.scalar(min_img_size))
    )

    if args.keywords is not None:
        keywords = [keyword.strip() for keyword in args.keywords.split(r",")]
        filter_regex = (
            "("
            + "|".join(
                ".*".join([f"\\b{keyword}\\b" for keyword in permutation])
                for permutation in permutations(keywords)
            )
            + ")"
        )
        filter_rules = filter_rules & pc.match_substring_regex(
            pc.field("TEXT"), filter_regex, ignore_case=True
        )

    num_processes_for_download = args.num_processes
    disallowed_header_directives = []
    size = (min_img_size, min_img_size)

    print(f"""Launching {num_processes_for_download} processes ...\n""")

    sem = BoundedSemaphore(num_processes_for_download)
    thread_pool = ThreadPool(num_processes_for_download)

    def entry_generator(file: str):
        """Generator to produce jobs for `download_task()`"""
        table = pq.read_table(file, columns=COLS, filters=filter_rules)
        print(f"""Found {table.num_rows} entries.""")
        for batch in table.to_batches():
            for _idx, row in batch.to_pandas().iterrows():
                sem.acquire()
                url, _width, _height, text = row.values
                yield url, text

    def download_task(*args):
        """This function will run in the number of processes given by `num_processes_for_download`.
        The processes are managed by Python's `multiprocessing` module.
        """
        ((url, text),) = args
        url, img_bytes, err_msg = download_image_with_retry(
            url,
            timeout=10,
            retries=0,
            disallowed_header_directives=disallowed_header_directives,
        )
        if img_bytes is None or err_msg is not None:
            return url, None, None, None, None, err_msg
        try:
            img = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
        except UnidentifiedImageError as e:
            return url, None, None, None, None, e
        original_width, original_height = img.size
        if original_height < min_img_size or original_width < min_img_size:
            return url, None, None, None, None, None
        edge_size = min(img.width, img.height)
        crop_left = (img.width - edge_size) // 2
        crop_upper = (img.height - edge_size) // 2
        img = img.crop(
            (crop_left, crop_upper, crop_left + edge_size, crop_upper + edge_size)
        ).resize(size, resample=Image.BICUBIC)
        img_stream = io.BytesIO()
        img.convert("RGB").save(img_stream, format="JPEG", quality=80, optimize=True)
        return url, img_stream, original_width, original_height, text, err_msg

    t0 = time.time()
    file_counter = 0
    schema = pa.schema(
        [
            pa.field("size", pa.uint32()),
            pa.field("width", pa.uint16()),
            pa.field("height", pa.uint16()),
            pa.field("original_width", pa.uint16()),
            pa.field("original_height", pa.uint16()),
            pa.field("url", pa.string()),
            pa.field("text", pa.string()),
            pa.field("jpg", pa.binary()),
            pa.field("license", pa.string()),
        ],
        metadata={
            "size": "size of file in bytes",
            "width": "width of image in pixels",
            "height": "height of image in pixels",
            "original_width": "original width of image in pixels",
            "original_height": "original height of image in pixels",
            "url": "origin of image",
            "text": "description of image",
            "jpg": "binary image data in JPEG format",
            "license": "license information",
        },
    )
    pq_writer = ParquetWriter(args.output, schema)

    for file in files:
        print(f"""Processing "{file}"\nwith filter `{filter_rules}` ...""")
        entries = thread_pool.imap_unordered(download_task, entry_generator(file))
        for url, img_stream, original_width, original_height, text, err_msg in entries:
            file_counter += 1
            if img_stream is None or err_msg is not None:
                sem.release()
                continue
            _filename, file_ext = os.path.splitext(urlparse(url).path)
            file_ext = file_ext[1:]
            if file_ext not in ALLOWED_EXTS:
                sem.release()
                continue
            img_bytes = img_stream.getvalue()
            hash = hashlib.blake2b(
                img_bytes, digest_size=16, usedforsecurity=False
            ).hexdigest()
            img_stream.seek(0)
            metadata = {
                "size": len(img_bytes),
                "width": size[0],
                "height": size[1],
                "original_width": original_width,
                "original_height": original_height,
                "url": quote(url),
                "text": text.replace('"', ""),
                "jpg": img_stream.getvalue(),
                "license": "?",
            }
            pq_writer.write(hash, metadata)
            print(
                f"""\r\u001b[32m{file_counter / (time.time() - t0):.2f}/s\u001b[0m""",
                end="",
            )
            sem.release()
        print("\r\u001b[K")
        with open(PROCESSED_FILE, "a+") as ready_file:
            print(file, file=ready_file)

    pq_writer.close()
    thread_pool.terminate()
    thread_pool.join()

    print(
        f"""READY.

total time:     {time.time() - t0:.1f} secs
total files:    {file_counter}
avg. files/sec: {file_counter / (time.time() - t0):.1f}
"""
    )


if __name__ == "__main__":
    main()
