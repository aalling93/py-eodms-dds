import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path("../satellite_downloader/.env")

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    raise FileNotFoundError(f".env file not found at {env_path}")

username = os.getenv("EODMS_USER")
password = os.getenv("EODMS_PASS")




from eodms_rapi import EODMSRAPI
from eodms_dds import DDSClient

rapi = EODMSRAPI(username, password)
dds = DDSClient(username=username, password=password, environment="prod")



# 1) Get items (with nice ETA bar)
items = client.get_items(item_uuids, catalog="EODMS", rate_limit_per_sec=2.0)

# 2) Download all available items (parallel, in_progress -> completed, optional unzip)
results = client.download_items(items, out_dir="/mnt/hdd/Data/EODMS/RCM", unzip=True, keep_zip=False, max_workers=3)

