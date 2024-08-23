import sys
import importlib
from lightningdb import LightningCtx, run_pipeline

if len(sys.argv) < 2:
    print("Usage: python runner.py <site>")
    sys.exit(1)

sitename = sys.argv[1]
site = importlib.import_module(f"quadcrawl.sites.{sitename}")
pipeline = site.pipeline

ctx = LightningCtx("/tmp/1.db", "/tmp/")
run_pipeline(ctx, sitename, pipeline, memorize=False)
