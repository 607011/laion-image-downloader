#!/usr/bin/env python3

import pyarrow.parquet as pq
from base64 import b64encode
from urllib.parse import unquote
import sys

def main():
    COLS = ["size", "width", "height", "original_width", "original_height", "url", "text", "jpg"]
    MAX_IMAGES = 1280
    parquet_filename = sys.argv[1]
    parquet_file = open(parquet_filename, "rb")
    table = pq.read_table(parquet_file, columns=COLS)
    print(f"""Found {table.num_rows} entries.""")
    index_filename = "index.html"
    index_file = open(index_filename, "w+")
    print(
        f"""<!DOCTYPE html>
<html>
  <head>
    <title>Sheep</title>
    <style>
      * {{
        padding: 0;
        margin: 0;
        box-sizing: border-box;
      }}
      html, body {{
        background-color: #222;
      }}
      #table {{
        display: flex;
        flex-direction: row;
        flex-wrap: wrap;
      }}
      #table>a {{
        display: inline-block;
        width: 64px;
        height: 64px;
        background-size: contain;
      }}
    </style>
  </head>
  <body>
    <main>
      <div id="table">
""",
        file=index_file,
    )

    def write_out():
        n = 0
        for batch in table.to_batches():
            for _idx, row in batch.to_pandas().iterrows():
                n += 1
                if n >= MAX_IMAGES:
                    return
                _size, _width, _height, original_width, original_height, url, text, jpg = row.values
                print(f"""        <a href="{unquote(url)}" title="{original_width}x{original_height}, {text.replace("\"", "")}" style="background-image: url('data:image/jpeg;base64,{b64encode(jpg).decode()}')"></a>""", file=index_file)


    write_out()

    print(f"""
      </div>
    </main>
  </body>
</html>
""",
        file=index_file,
    )
        


if __name__ == "__main__":
    main()
