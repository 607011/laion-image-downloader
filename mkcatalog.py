#!/usr/bin/env python3

"""
Catalog Builder - Generate an HTML file containing images stored in a parquet
file created by dl.py

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

from argparse import ArgumentParser
import pyarrow.parquet as pq
import pyarrow as pa
from base64 import b64encode
from urllib.parse import unquote
from jinja2 import Environment, PackageLoader


def links(table: pa.Table, max_images: int):
    n = 0
    for batch in table.to_batches():
        for _idx, row in batch.to_pandas().iterrows():
            n += 1
            if n == max_images:
                return
            (
                original_width,
                original_height,
                url,
                text,
                jpg,
                hash,
            ) = row.values
            yield {
                "url": unquote(url),
                "original_width": original_width,
                "original_height": original_height,
                "text": text.replace('"', ""),
                "hash": hash,
                "jpg": b64encode(jpg).decode(),
            }


def main():
    COLS = ["original_width", "original_height", "url", "text", "jpg", "hash"]
    MAX_IMAGES = 1280
    INDEX_HTML = "index.html"
    DEFAULT_TITLE = "Catalog"

    ap = ArgumentParser(
        prog="LAION image catalog generator",
        description="Create a catalog of images, i.e. a single HTML file with images embedded as Data URLs, contained in a parquet file.",
    )
    ap.add_argument(
        "parquet",
        type=str,
        help="LAION parquet file to read.",
    )
    ap.add_argument(
        "-O",
        "--output",
        type=str,
        default=INDEX_HTML,
        help=f"""Write HTML file with catalog to OUTPUT (default: {INDEX_HTML})""",
    )
    ap.add_argument(
        "-T",
        "--title",
        type=str,
        default=DEFAULT_TITLE,
        help=f"""Title of HTML page (default: {DEFAULT_TITLE})""",
    )
    ap.add_argument(
        "-n",
        "--max-images",
        type=int,
        default=MAX_IMAGES,
        help=f"""maximum number of images in catalog (default: {MAX_IMAGES})""",
    )

    args = ap.parse_args()

    jenv = Environment(loader=PackageLoader("catalog"))
    template = jenv.get_template("index.html")

    parquet_file = open(args.parquet, "rb")
    table = pq.read_table(parquet_file, columns=COLS)
    print(f"""Found {table.num_rows} entries.""")
    print(
        f"""Generating {args.output} with {min(table.num_rows, args.max_images)} images ... """,
        end="",
        flush=True,
    )
    keywords = table.schema.metadata.get(b"keywords", b"").decode()

    with open(args.output, "w+") as index_file:
        print(
            template.render(
                keywords=keywords, title=keywords, links=links(table, args.max_images)
            ),
            file=index_file,
        )

    print("\nReady.")

if __name__ == "__main__":
    main()
