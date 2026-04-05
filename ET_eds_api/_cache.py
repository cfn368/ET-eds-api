import hashlib
import os
import pandas as pd
import requests


def fetch(url, params, cache=False, cache_dir="eds_cache"):
    if cache:
        os.makedirs(cache_dir, exist_ok=True)
        key = hashlib.md5((url + str(sorted(params.items()))).encode()).hexdigest()
        path = os.path.join(cache_dir, f"{key}.parquet")
        if os.path.exists(path):
            return pd.read_parquet(path)

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    df = pd.DataFrame(r.json().get("records", []))

    if cache:
        df.to_parquet(path, index=False)

    return df
