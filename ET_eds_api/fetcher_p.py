import json
import pandas as pd
import requests


# ==================== ==================== ==================== ====================
# 1. Get weighted price h
def get_wp_h(
        start   = int,
        end     = int,
        EA      = ["DK1","DK2"],
):
    ea_json = json.dumps(EA)  # ["DK1","DK2"] -> '["DK1","DK2"]'

    # 1. prices
    BASE = "https://api.energidataservice.dk/dataset/Elspotprices"
    params = {
        "start":    start,
        "end":      end,
        "timezone": "UTC",
        "filter":   f'{{"PriceArea":{ea_json}}}',
        "columns":  "HourUTC,PriceArea,SpotPriceDKK",
        "sort":     "HourUTC asc",
        "limit":    0
    }
    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    p_h = pd.DataFrame(r.json().get("records", []))
    p_h["HourUTC"] = pd.to_datetime(p_h["HourUTC"])

    # 2. after 2025-09-30
    BASE = "https://api.energidataservice.dk/dataset/DayAheadPrices"
    params = {
        "start":    start,
        "end":      end,
        "timezone": "UTC",
        "filter":   f'{{"PriceArea":{ea_json}}}',
        "columns":  "TimeUTC,PriceArea,DayAheadPriceDKK",
        "sort":     "TimeUTC asc",
        "limit":    0
    }

    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    p_h_ = pd.DataFrame(r.json().get("records", []))

    # 3. get hour and take hourly average (DayAheadPrices may be empty for older ranges)
    if not p_h_.empty:
        p_h_["TimeUTC"] = pd.to_datetime(p_h_["TimeUTC"])
        p_h_["hour"] = p_h_["TimeUTC"].dt.to_period("h").dt.to_timestamp(how="start")
        p_h_hourly = (
            p_h_.groupby(["hour", "PriceArea"])["DayAheadPriceDKK"]
            .mean()
            .reset_index()
            .rename(columns={"hour": "HourUTC", "DayAheadPriceDKK": "SpotPriceDKK"})
        )
    else:
        p_h_hourly = pd.DataFrame(columns=["HourUTC", "PriceArea", "SpotPriceDKK"])

    # 4. stack with the older data
    p_combined = pd.concat([
        p_h[["HourUTC", "PriceArea", "SpotPriceDKK"]],
        p_h_hourly
    ], ignore_index=True).sort_values(["HourUTC", "PriceArea"])

    # 5. get quantities
    base = "https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement"
    value_columns = ['GrossConsumptionMWh']

    params = {
        "start":    start,
        "end":      end,
        "timezone": "UTC",
        "columns":  "HourUTC,PriceArea," + ",".join(value_columns),
        "sort":     "HourUTC asc",
        "limit":    0,
    }

    r = requests.get(base, params=params, timeout=60)
    r.raise_for_status()
    q_h_EA = pd.DataFrame(r.json().get("records", []))
    q_h_EA["HourUTC"] = pd.to_datetime(q_h_EA["HourUTC"])

    # 6. merge in quantities for 2025
    p_combined = p_combined.merge(
        q_h_EA[["HourUTC", "PriceArea", "GrossConsumptionMWh"]],
        on=["HourUTC", "PriceArea"],
        how="left",
    )

    # 7. compute weighted average
    p_combined["WeightedPrice"] = p_combined["SpotPriceDKK"] * p_combined["GrossConsumptionMWh"]

    wp_h = (
        p_combined.groupby("HourUTC")
        .apply(lambda g: g["WeightedPrice"].sum() / g["GrossConsumptionMWh"].sum())
        .rename("SpotPriceDKK_weighted")
        .reset_index()
    )

    q_h = (
        q_h_EA.groupby("HourUTC")["GrossConsumptionMWh"]
        .sum()
        .reset_index()
    )

    return wp_h, q_h


# ==================== ==================== ==================== ====================
# 2. aggregate wp to m or y or w
def wagg_wp(start, end):

    # 1. get weighted DK prices hourly for period
    wp_h, q_h = get_wp_h(start=start, end=end)

    # 2. merge price and quantity
    wq = wp_h.merge(q_h, on="HourUTC")

    # 3. daily
    wq["date"] = wq["HourUTC"].dt.to_period("D")
    wp_d = (
        wq.groupby("date")
        .apply(lambda g: (g["SpotPriceDKK_weighted"] * g["GrossConsumptionMWh"]).sum() / g["GrossConsumptionMWh"].sum())
        .rename("SpotPriceDKK_weighted")
        .reset_index()
    )

    # 4. weekly
    wq["week"] = wq["HourUTC"].dt.to_period("W")
    wp_w = (
        wq.groupby("week")
        .apply(lambda g: (g["SpotPriceDKK_weighted"] * g["GrossConsumptionMWh"]).sum() / g["GrossConsumptionMWh"].sum())
        .rename("SpotPriceDKK_weighted")
        .reset_index()
    )

    # 5. monthly
    wq["month"] = wq["HourUTC"].dt.to_period("M")
    wp_m = (
        wq.groupby("month")
        .apply(lambda g: (g["SpotPriceDKK_weighted"] * g["GrossConsumptionMWh"]).sum() / g["GrossConsumptionMWh"].sum())
        .rename("SpotPriceDKK_weighted")
        .reset_index()
    )

    # 6. yearly
    wq["year"] = wq["HourUTC"].dt.year
    wp_y = (
        wq.groupby("year")
        .apply(lambda g: (g["SpotPriceDKK_weighted"] * g["GrossConsumptionMWh"]).sum() / g["GrossConsumptionMWh"].sum())
        .rename("SpotPriceDKK_weighted")
        .reset_index()
    )

    return wp_d, wp_w, wp_m, wp_y
