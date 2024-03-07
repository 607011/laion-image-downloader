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

from dataclasses import dataclass
from PIL import Image, UnidentifiedImageError
from multiprocessing.pool import ThreadPool
from threading import BoundedSemaphore
from itertools import permutations
from tqdm import tqdm
from typing import Generator, Self
from warnings import warn
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from urllib.parse import urlparse, quote
import argparse
import hashlib
import io
import os
import time
import sys
import yaml
from downloadhelper import download_image_with_retry
from writer import ParquetWriter


@dataclass
class ImgData:
    url: str
    stream: io.IOBase
    original_width: int
    original_height: int
    text: str
    err_msg: str

    def __init__(
        self,
        url: str,
        stream: io.IOBase,
        original_width: int,
        original_height: int,
        text: str,
        err_msg: str,
    ) -> Self:
        self.url = url
        self.stream = stream
        self.original_width = original_width
        self.original_height = original_height
        self.text = text
        self.err_msg = err_msg


def main() -> None:
    PROCESSED_FILE: str = "processed.txt"
    ALLOWED_EXTS: list[str] = ["jpg", "png"]
    NUM_THREADS_DOWNLOAD: int = 8 * os.cpu_count()
    MIN_IMG_SIZE: int = 128

    cfg_original_width = "original_width"
    cfg_original_height = "original_height"
    cfg_keywords = "caption"
    cfg_url = "url"
    cols_to_read = [cfg_url, cfg_original_width, cfg_original_height, cfg_keywords]

    class ConfigReader(argparse.Action):
        def __call__(self, _parser, _namespace, filename, _option_string=None):
            nonlocal cfg_original_width, cfg_original_height, cfg_keywords, cfg_url, cols_to_read

            config = yaml.load(open(filename, "r"), Loader=yaml.Loader)
            if config["field_names"]["original_width"]:
                cfg_original_width = config["field_names"]["original_width"]
            if config["field_names"]["original_height"]:
                cfg_original_height = config["field_names"]["original_height"]
            if config["field_names"]["keywords"]:
                cfg_keywords = config["field_names"]["keywords"]
            if config["field_names"]["url"]:
                cfg_url = config["field_names"]["url"]
            cols_to_read = [
                cfg_url,
                cfg_original_width,
                cfg_original_height,
                cfg_keywords,
            ]

    ap = argparse.ArgumentParser(
        prog="laion-dl",
        description="Downloader for images referenced by LAION parquet files",
    )
    ap.add_argument(
        "--config",
        action=ConfigReader,
        help="",
    )
    ap.add_argument(
        "parquet",
        nargs="+",
        type=str,
        help="LAION parquet file(s) to read.",
    )
    ap.add_argument(
        "-O",
        "--output",
        type=str,
        default="images.parquet",
        help="Write resulting parquet file to OUTPUT",
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
        help=f"""images should have a width and height of at least NUM_PIXELS (default: {MIN_IMG_SIZE})""",
        metavar="NUM_PIXELS",
    )
    ap.add_argument(
        "--no-continue",
        action="store_true",
        help="ignore files written to processed.txt, instead begin from scratch",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="don't download or store anything",
    )
    args = ap.parse_args()

    dry_run = args.dry_run

    print("\u001b[1mLAION image downloader.\u001b[0m\n")

    min_img_size = args.min_img_size

    files = set(args.parquet)

    if args.no_continue:
        try:
            os.remove(PROCESSED_FILE)
        except FileNotFoundError:
            pass
        except OSError:
            print(f"""{PROCESSED_FILE} is a directory. Exiting ...""", file=sys.stderr)
            sys.exit(1)
    elif os.path.isfile(PROCESSED_FILE):
        processed = {file.strip() for file in open(PROCESSED_FILE, "r").readlines()}
        files = files.difference(processed)

    if len(files) == 0:
        print("Nothing to do.")
        return

    filter_rules = (pc.field(cfg_original_width) >= pc.scalar(min_img_size)) & (
        pc.field(cfg_original_height) >= pc.scalar(min_img_size)
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
            pc.field(cfg_keywords), filter_regex, ignore_case=True
        )
    else:
        keywords = None

    num_processes_for_download = args.num_processes
    disallowed_header_directives = []
    size = (min_img_size, min_img_size)

    print(f"""Launching {num_processes_for_download} processes ...\n""")

    sem = BoundedSemaphore(num_processes_for_download)
    thread_pool = ThreadPool(num_processes_for_download)

    def entry_generator(
        file: str,
    ) -> Generator[tuple[str | None, str | None], None, None]:
        """Generator to produce jobs for `download_task()`"""
        try:
            table = pq.read_table(file, columns=cols_to_read, filters=filter_rules)
        except FileNotFoundError:
            warn(f"""WARNING: file not found ({file})""")
            yield None, None
        for batch in table.to_batches():
            for _idx, row in batch.to_pandas().iterrows():
                sem.acquire()
                url, _width, _height, text = row.values
                yield url, text

    def download_task(*args) -> ImgData:
        """This function will run in the number of processes given by `num_processes_for_download`.
        The processes are managed by Python's `multiprocessing` module.
        """
        if dry_run:
            return None
        ((url, text),) = args
        if url is None or text is None:
            return None
        url, img_bytes, err_msg = download_image_with_retry(
            url,
            timeout=10,
            retries=0,
            disallowed_header_directives=disallowed_header_directives,
        )
        if img_bytes is None or err_msg is not None:
            return ImgData(url, None, None, None, None, err_msg)
        try:
            img = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
        except UnidentifiedImageError as e:
            return ImgData(url, None, None, None, None, e)
        original_width, original_height = img.size
        if original_height < min_img_size or original_width < min_img_size:
            return ImgData(url, None, None, None, None, None)
        edge_size = min(img.width, img.height)
        crop_left = (img.width - edge_size) // 2
        crop_upper = (img.height - edge_size) // 2
        img = img.crop(
            (crop_left, crop_upper, crop_left + edge_size, crop_upper + edge_size)
        ).resize(size, resample=Image.BICUBIC)
        img_stream = io.BytesIO()
        img.convert("RGB").save(img_stream, format="JPEG", quality=80, optimize=True)
        return ImgData(url, img_stream, original_width, original_height, text, err_msg)

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
            pa.field("hash", pa.string()),
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
            "hash": "16 bytes Blake2 hash in hex format",
            "keywords": " ".join(keywords),
            "creator": "https://github.com/607011/laion-image-downloader",
        },
    )

    if os.path.isfile(args.output) and args.no_continue:
        print(f"""Output file '{args.output} already exists.""")
        choice = input(
            "Do you want to [a]bort, or [d]elete the file beforce proceeding? "
        )
        if choice == "d":
            os.remove(args.output)
        else:
            sys.exit(1)

    t0 = time.time()
    with ParquetWriter(args.output, schema, buffer_size=256) as pq_writer:
        file_counter = 0
        for file in tqdm(files, desc="parquets ", unit="file", position=0):
            entries = thread_pool.imap_unordered(download_task, entry_generator(file))
            for img in tqdm(entries, desc="downloads", unit="", position=1, leave=False):
                if img is None:
                    continue
                file_counter += 1
                if img.stream is None or img.err_msg is not None:
                    sem.release()
                    continue
                _filename, file_ext = os.path.splitext(urlparse(img.url).path)
                file_ext = file_ext[1:]
                if file_ext not in ALLOWED_EXTS:
                    sem.release()
                    continue
                img_bytes = img.stream.getvalue()
                hash = hashlib.blake2b(
                    img_bytes, digest_size=16, usedforsecurity=False
                ).hexdigest()
                img.stream.seek(0)
                metadata = {
                    "size": len(img_bytes),
                    "width": size[0],
                    "height": size[1],
                    "original_width": img.original_width,
                    "original_height": img.original_height,
                    "url": quote(img.url),
                    "text": img.text.replace('"', ""),
                    "jpg": img.stream.getvalue(),
                    "license": "?",
                    "hash": hash,
                }
                pq_writer.write(metadata)
                sem.release()
            with open(PROCESSED_FILE, "a+") as ready_file:
                print(file, file=ready_file)

    thread_pool.terminate()
    thread_pool.join()
    os.remove(PROCESSED_FILE)

    print(
        f"""
READY.

total time:     {time.time() - t0:.1f} secs
total files:    {file_counter}
avg. files/sec: {file_counter / (time.time() - t0):.1f}
"""
    )


if __name__ == "__main__":
    main()
