from lightningdb import *
from quadcrawl.sitemap_util import parse_sitemap, schema_urls

pipeline = [
    Const(
        items=[{"url": "https://apps.apple.com/sitemaps_apps_index_app_1.xml"}],
        avro_schema=schema_urls,
    ),
    Fetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),
    Shuffle(nparts=100, key="url", avro_schema=schema_urls),
    Fetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),
    Fetch(),
]
