import hashlib
import os
import pandas as pd
import requests


def write_ep_txt(series, filename, folder="variation_patterns"):
    """Write a pandas Series to a plain text file, one value per line.

    Creates *folder* if it does not exist.  Returns the full file path.
    Raises ValueError if the series contains NaN values.
    """
    n_nan = series.isna().sum()
    if n_nan:
        raise ValueError(
            f"Series contains {n_nan} NaN value(s) — fill or drop them before saving."
        )
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w") as fh:
        for val in series:
            fh.write(f"{val:.2f}".replace(".", ",") + "\n")
    print(f"Saved {len(series)}-row txt: {path}")
    return path


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
