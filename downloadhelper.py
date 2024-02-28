"""
Copyright (c) 2021 Romain Beaumont
See https://github.com/rom1504/img2dataset/blob/main/img2dataset/downloader.py
Modified 2024 by Oliver Lau
"""

import urllib.request
import traceback


def _is_disallowed(headers, disallowed_header_directives):
    """Check if HTTP headers contain an X-Robots-Tag directive disallowing usage"""
    for values in headers.get_all("X-Robots-Tag", []):
        try:
            uatoken_directives = values.split(":", 1)
            directives = [x.strip().lower() for x in uatoken_directives[-1].split(",")]
            if any(x in disallowed_header_directives for x in directives):
                return True
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"Failed to parse X-Robots-Tag: {values}: {err}")
    return False


def _download_image(url, timeout, disallowed_header_directives):
    """Download an image with urllib"""
    img_bytes = None
    user_agent_string = (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
    )
    try:
        request = urllib.request.Request(
            url, data=None, headers={"User-Agent": user_agent_string}
        )
        with urllib.request.urlopen(request, timeout=timeout) as r:
            img_bytes = r.read()
            if disallowed_header_directives and _is_disallowed(
                r.headers,
                disallowed_header_directives,
            ):
                return url, None, "Use of image disallowed by X-Robots-Tag directive"
        return url, img_bytes, None
    except Exception as err:  # pylint: disable=broad-except
        return url, None, str(err)


def download_image_with_retry(url, timeout, retries, disallowed_header_directives):
    for _ in range(retries + 1):
        url, img_bytes, err = _download_image(
            url, timeout, disallowed_header_directives
        )
        if img_bytes is not None:
            return url, img_bytes, err
    return url, None, err
