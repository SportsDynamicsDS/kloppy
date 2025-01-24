import json
from typing import IO, Optional

from .base import SecondSpectrumParser
from .insight import InsightParser
from .metadata_json import MetadataJSONParser


def get_event_parser(
    feed: IO[bytes],
    feed_code: Optional[str] = None,
    **kwargs,
) -> SecondSpectrumParser:
    # Infer the feed code if not provided
    if feed_code is None:
        first_dict = json.loads(feed.readline().decode("utf-8"))
        if "optaEvent" in first_dict:
            feed_code = "Insight"
        # Placeholder for Dragon feed code
        else:
            raise NotImplementedError(
                "A parser for these event feeds is not yet implemented."
            )
        feed.seek(0)

    if feed_code == "Insight":
        return InsightParser(feed)
    # Placeholder for DragonParser
    else:
        raise NotImplementedError(
            f"A parser for {feed_code} feeds is not yet implemented."
        )


def get_metadata_parser(
    feed: IO[bytes],
    feed_format: Optional[str] = None,
    **kwargs,
) -> SecondSpectrumParser:
    # infer the data format if not provided
    if feed_format is None:
        first_char = feed.read(1).decode("utf-8")
        feed_format = "XML" if first_char == "<" else "JSON"
        feed.seek(0)

    feed_format = feed_format.upper()
    if feed_format == "JSON":
        return MetadataJSONParser(feed)
    # Placeholder for XML metadata feed format
    else:
        raise NotImplementedError(
            f"A parser for metadata feeds in {feed_format} format is not yet implemented."
        )


__all__ = [
    "get_event_parser",
    "get_metadata_parser",
    "SecondSpectrumParser",
]
