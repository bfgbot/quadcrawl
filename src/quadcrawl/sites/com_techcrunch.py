from lightningdb import *
from quadcrawl.sitemap_util import parse_sitemap, schema_urls

pipeline = [
    Const(
        items=[{"url": "https://techcrunch.com/sitemap.xml"}],
        avro_schema=schema_urls,
    ),
    Fetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),
    Fetch(),
    FlatMap(fn=parse_sitemap, avro_schema=schema_urls),
    Sql(sql=r"select * from input where match(url, '^https://techcrunch.com/\\d{4}/')"),
    Shuffle(nparts=10, key="url", avro_schema=schema_urls),
    Fetch(),
]
