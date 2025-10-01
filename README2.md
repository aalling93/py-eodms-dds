client = DDSClient(username, password)

# 1) Get items (with nice ETA bar)
items = client.get_items(item_uuids, catalog="EODMS", rate_limit_per_sec=2.0)

# 2) Download all available items (parallel, in_progress -> completed, optional unzip)
results = client.download_items(items, out_dir="/mnt/hdd/Data/EODMS/RCM", unzip=True, keep_zip=False, max_workers=3)

