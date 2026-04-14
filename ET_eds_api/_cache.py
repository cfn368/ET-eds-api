import hashlib
import os
import pandas as pd
import requests


def _to_ep_year(values, timestamps, weights=None):
    """Reduce an hourly series to one representative year of 8784 rows.

    Steps:
      1. Drop Feb 29 (leap day) rows.
      2. Group by (month, day, hour) and average across years.
         If *weights* are provided the average is consumption-weighted.
      3. Assert exactly 8760 slots (365 × 24), then append the first 24 hours
         so the result is 8784 rows as required by EnergyPLAN.
    """
    df = pd.DataFrame({
        "ts":  pd.to_datetime(timestamps).values,
        "val": pd.to_numeric(values, errors="coerce").values,
    })
    if weights is not None:
        df["w"] = pd.to_numeric(weights, errors="coerce").values

    # 1. drop Feb 29
    df = df[~((df["ts"].dt.month == 2) & (df["ts"].dt.day == 29))].copy()

    # 2. hour-of-year key — unique across years
    df["hoy"] = df["ts"].dt.month * 10000 + df["ts"].dt.day * 100 + df["ts"].dt.hour

    if weights is not None:
        df["wval"] = df["val"] * df["w"]
        num = df.groupby("hoy", sort=True)["wval"].sum()
        den = df.groupby("hoy", sort=True)["w"].sum()
        agg = (num / den).rename("val").reset_index()
    else:
        agg = df.groupby("hoy", sort=True)["val"].mean().reset_index()

    if len(agg) != 8760:
        raise ValueError(
            f"Expected 8760 hour-of-year slots after removing Feb 29, got {len(agg)}. "
            "Make sure the input covers complete calendar years."
        )

    # 3. pad with first day → 8784
    result = list(agg["val"]) + list(agg["val"].iloc[:24])
    return result


def write_ep_txt(values, timestamps, filename, folder="variation_patterns", weights=None):
    """Aggregate an hourly series and write an EnergyPLAN-ready txt file (8784 rows).

    - Drops Feb 29 before averaging so leap years are handled cleanly.
    - If the series spans multiple years, averages per hour-of-year
      (consumption-weighted when *weights* are supplied).
    - Appends the first 24 hours to reach 8784 rows.
    - Writes comma decimal separator, 2 decimal places.
    """
    ep_vals = _to_ep_year(values, timestamps, weights)

    n_nan = sum(1 for v in ep_vals if pd.isna(v))
    if n_nan:
        raise ValueError(f"Aggregated series contains {n_nan} NaN value(s).")

    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w") as fh:
        for val in ep_vals:
            fh.write(f"{val:.2f}".replace(".", ",") + "\n")
    print(f"Saved {len(ep_vals)}-row EnergyPLAN txt: {path}")
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
