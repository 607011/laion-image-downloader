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
from base64 import b64encode
from urllib.parse import unquote


def main():
    COLS = [
        "original_width",
        "original_height",
        "url",
        "text",
        "jpg",
        "hash"
    ]
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

    parquet_file = open(args.parquet, "rb")
    table = pq.read_table(parquet_file, columns=COLS)
    print(f"""Found {table.num_rows} entries.""")
    index_file = open(args.output, "w+")
    keywords = table.schema.metadata.get(b"keywords", "").decode()
    print(
        f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>{keywords}</title>
    <meta name="keywords" content="{keywords}">
    <meta name="generator" content="https://github.com/607011/laion-image-downloader/blob/main/mkcatalog.py">
    <link rel="stylesheet" href="catalog.css">
    <script src="catalog.js"></script>
  </head>
  <body>
    <main>
      <div id="table">""",
        file=index_file,
    )

    def write_out():
        n = 0
        for batch in table.to_batches():
            for _idx, row in batch.to_pandas().iterrows():
                n += 1
                if n == args.max_images:
                    return
                (
                    original_width,
                    original_height,
                    url,
                    text,
                    jpg,
                    hash,
                ) = row.values
                print(
                    f"""        <a href="{unquote(url)}" """
                    f"""title="{original_width}x{original_height}, """
                    f"""{text.replace("\"", "")}" """
                    f"""data-hash="{hash}" """
                    f"""style="background-image: url('data:image/jpeg;base64,"""
                    f"""{b64encode(jpg).decode()}')"></a>""",
                    file=index_file,
                )

    write_out()

    print(
        f"""      </div>
      <div id="stats"><div>selected: <span id="select-count">0</span></div></div>
    </main>

  </body>
</html>""",
        file=index_file,
    )


if __name__ == "__main__":
    main()
