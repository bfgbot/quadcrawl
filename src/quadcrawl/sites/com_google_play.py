from lightningdb import *
from quadcrawl.sitemap_util import parse_sitemap, schema_urls

pipeline = [
    Const(
        items=[
            {"url": "https://play.google.com/sitemaps/sitemaps-index-0.xml"},
            {"url": "https://play.google.com/sitemaps/sitemaps-index-1.xml"},
        ],
        avro_schema=schema_urls,
    ),  # size=2
    MultiFetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),  # size=70443
    Shuffle(nparts=7000, key="url", avro_schema=schema_urls),
    MultiFetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),  # size=47586618
    MultiFetch(),
]
