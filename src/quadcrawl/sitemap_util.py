import gzip
from typing import TypedDict

from lxml import etree


def get_locs(xmldata: bytes) -> list[str]:
    assert isinstance(xmldata, bytes)

    tree = etree.fromstring(xmldata)  # type: ignore

    ns = {"sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = tree.xpath("//sitemap:loc", namespaces=ns)
    return [loc.text for loc in locs]


def get_locs_unzip(compressed_data: bytes) -> list[str]:
    assert isinstance(compressed_data, bytes)

    return get_locs(gzip.decompress(compressed_data))


schema_urls = {
    "type": "record",
    "name": "urls",
    "fields": [
        {"name": "url", "type": "string"},
    ],
}


class FetchResult(TypedDict):
    content: bytes
    url: str


class SitemapUrl(TypedDict):
    url: str


def parse_sitemap(row: FetchResult) -> list[SitemapUrl]:
    sitemap_content = row["content"]
    sitemap_url = row["url"]
    assert isinstance(sitemap_content, bytes)

    try:
        if sitemap_url.endswith(".gz"):
            urls = get_locs_unzip(sitemap_content)
        else:
            urls = get_locs(sitemap_content)
        return [{"url": url} for url in urls]
    except Exception as e:
        print("invalid sitemap", sitemap_url)
        return []
