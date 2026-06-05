import json
import pandas as pd
from ._cache import fetch, write_ep_txt

# ==================== ==================== ==================== ====================
# 0. helper — show available value_columns and cap_column options
def columns():
    prod_cols = fetch(
        "https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement",
        {"limit": 1},
    ).columns.tolist()

    cap_cols = fetch(
        "https://api.energidataservice.dk/dataset/CapacityPerMunicipality",
        {"limit": 1},
    ).columns.tolist()

    value_options = [c for c in prod_cols if c.endswith("_MWh")]
    cap_options   = [c for c in cap_cols  if "Capacity" in c]

    print("── value_columns (ProductionConsumptionSettlement) ──")
    for c in value_options:
        print(f"  {c}")

    print("\n── cap_column (CapacityPerMunicipality) ──")
    for c in cap_options:
        print(f"  {c}")


# ==================== ==================== ==================== ====================
# 1. build VE VP
def VE(value_columns, cap_column, col_name, start, end, verbose=True, no_index=False, cap_ref=None, cache=False, cache_dir="eds_cache", save_txt=False, EP_style=True):

    # 1. get variation ====================
    # 1.1 get
    base = "https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement"

    params = {
        "start":    start,
        "end":      end,
        "timezone": "UTC",
        "columns":  "HourUTC,PriceArea," + ",".join(value_columns),
        "sort":     "HourUTC asc",
        "limit":    0,
    }

    df = fetch(base, params, cache=cache, cache_dir=cache_dir)

    # 1.2 subtract leap year
    df["HourUTC"] = pd.to_datetime(df["HourUTC"])

    # 1.3 merge and prep
    for c in value_columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["value"] = df[list(value_columns)].sum(axis=1, min_count=1)

    df["month"] = df["HourUTC"].dt.to_period("M").dt.to_timestamp(how="start")

    df_val = df[['HourUTC','month','value']]
    df_val = df.groupby(["HourUTC", "month"])["value"].sum().reset_index()

    # 2. capacity index ====================
    # 2.1 get
    base = 'https://api.energidataservice.dk/dataset/CapacityPerMunicipality'

    params = {
        "start":    start,
        "end":      end,
        "columns":  "Month,MunicipalityNo," + ",".join(cap_column),
        "sort":     "Month asc",
        "limit":    0,
    }

    df_ind = fetch(base, params, cache=cache, cache_dir=cache_dir)

    dt = pd.to_datetime(df_ind["Month"], errors="coerce")
    df_ind["month"] = (
        dt.dt.to_period("M")
        .dt.to_timestamp(how="start")
    )

    # 2.2 drop double entries
    df_ind = df_ind.drop_duplicates(subset=["month", "MunicipalityNo"], keep="first")

    # 2.3 aggregate to months
    cap_col = cap_column[0]
    indx_m = (
        df_ind.groupby('month', as_index=True)
        .agg(**{cap_col: (cap_col, "sum")})
        .reset_index()
    )

    # 2.4 build index — denominator is cap_ref if provided, else last observed capacity
    denominator = cap_ref if cap_ref is not None else indx_m.iloc[-1][cap_col]
    indx_m[f'{col_name}_idx'] = indx_m[cap_col] / denominator

    if no_index:
        indx_m[f'{col_name}_idx'] = 1

    if verbose:
        print(f'\nIndex denominator is: ===================\n')
        print(f'  {cap_col}: {denominator}{"  (counterfactual)" if cap_ref is not None else "  (last observed)"}')

    # 3. merge and deflate
    df_ve = df_val.merge(indx_m[[f'{col_name}_idx','month']], on='month', how='left')
    df_ve[col_name] = df_ve.value / df_ve[f'{col_name}_idx']

    if save_txt:
        write_ep_txt(
            values     = df_ve[col_name],
            timestamps = df_ve["HourUTC"],
            filename   = f"{col_name}_{start}_{end}.txt",
            EP_style   = EP_style,
        )

    return df_ve


# ==================== ==================== ==================== ====================
# 2. batch run — one fetch per dataset regardless of number of technologies
def VE_run(specs, start, end, include_wp=True, EA=["DK1", "DK2"], verbose=True, cache=False, cache_dir="eds_cache", save_txt=False, EP_style=True):

    # collect all columns needed across specs
    all_value_cols = list(dict.fromkeys(c for s in specs for c in s["value_columns"]))
    all_cap_cols   = list(dict.fromkeys(s["cap_column"][0] for s in specs))

    prod_cols = all_value_cols + (["GrossConsumptionMWh"] if include_wp else [])

    # fetch 1: ProductionConsumptionSettlement — all technologies + consumption in one call
    df_prod = fetch(
        "https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement",
        {
            "start":    start,
            "end":      end,
            "timezone": "UTC",
            "columns":  "HourUTC,PriceArea," + ",".join(prod_cols),
            "sort":     "HourUTC asc",
            "limit":    0,
        },
        cache=cache, cache_dir=cache_dir,
    )
    df_prod["HourUTC"] = pd.to_datetime(df_prod["HourUTC"])
    df_prod["month"]   = df_prod["HourUTC"].dt.to_period("M").dt.to_timestamp(how="start")
    for c in prod_cols:
        df_prod[c] = pd.to_numeric(df_prod[c], errors="coerce")

    # fetch 2: CapacityPerMunicipality — all cap columns in one call
    df_cap = fetch(
        "https://api.energidataservice.dk/dataset/CapacityPerMunicipality",
        {
            "start":    start,
            "end":      end,
            "columns":  "Month,MunicipalityNo," + ",".join(all_cap_cols),
            "sort":     "Month asc",
            "limit":    0,
        },
        cache=cache, cache_dir=cache_dir,
    )
    df_cap["month"] = (
        pd.to_datetime(df_cap["Month"], errors="coerce")
        .dt.to_period("M")
        .dt.to_timestamp(how="start")
    )
    df_cap = df_cap.drop_duplicates(subset=["month", "MunicipalityNo"], keep="first")

    # compute VE for each spec from already-fetched data
    results = {}
    for s in specs:
        col_name      = s["col_name"]
        value_columns = s["value_columns"]
        cap_col       = s["cap_column"][0]
        no_index      = s.get("no_index", False)
        cap_ref       = s.get("cap_ref", None)

        df_v          = df_prod.copy()
        df_v["value"] = df_v[value_columns].sum(axis=1, min_count=1)
        df_val        = df_v.groupby(["HourUTC", "month"])["value"].sum().reset_index()

        indx_m = (
            df_cap.groupby("month", as_index=True)
            .agg(**{cap_col: (cap_col, "sum")})
            .reset_index()
        )
        denominator              = cap_ref if cap_ref is not None else indx_m.iloc[-1][cap_col]
        indx_m[f"{col_name}_idx"] = indx_m[cap_col] / denominator

        if no_index:
            indx_m[f"{col_name}_idx"] = 1

        if verbose:
            print(f"\nIndex denominator [{col_name}]: ===================")
            print(f"  {cap_col}: {denominator}{'  (counterfactual)' if cap_ref is not None else '  (last observed)'}")

        df_ve          = df_val.merge(indx_m[[f"{col_name}_idx", "month"]], on="month", how="left")
        df_ve[col_name] = df_ve["value"] / df_ve[f"{col_name}_idx"]

        if save_txt:
            write_ep_txt(
                values     = df_ve[col_name],
                timestamps = df_ve["HourUTC"],
                filename   = f"{col_name}_{start}_{end}.txt",
                EP_style   = EP_style,
            )

        results[col_name] = df_ve

    # optional: weighted price + quantity — reuses df_prod, no extra settlement fetch
    if include_wp:
        ea_json = json.dumps(EA)

        p_h = fetch(
            "https://api.energidataservice.dk/dataset/Elspotprices",
            {
                "start":    start,
                "end":      end,
                "timezone": "UTC",
                "filter":   f'{{"PriceArea":{ea_json}}}',
                "columns":  "HourUTC,PriceArea,SpotPriceDKK",
                "sort":     "HourUTC asc",
                "limit":    0,
            },
            cache=cache, cache_dir=cache_dir,
        )
        p_h["HourUTC"] = pd.to_datetime(p_h["HourUTC"])

        p_h_ = fetch(
            "https://api.energidataservice.dk/dataset/DayAheadPrices",
            {
                "start":    start,
                "end":      end,
                "timezone": "UTC",
                "filter":   f'{{"PriceArea":{ea_json}}}',
                "columns":  "TimeUTC,PriceArea,DayAheadPriceDKK",
                "sort":     "TimeUTC asc",
                "limit":    0,
            },
            cache=cache, cache_dir=cache_dir,
        )

        if not p_h_.empty:
            p_h_["TimeUTC"] = pd.to_datetime(p_h_["TimeUTC"])
            p_h_["hour"]    = p_h_["TimeUTC"].dt.to_period("h").dt.to_timestamp(how="start")
            p_h_hourly = (
                p_h_.groupby(["hour", "PriceArea"])["DayAheadPriceDKK"]
                .mean()
                .reset_index()
                .rename(columns={"hour": "HourUTC", "DayAheadPriceDKK": "SpotPriceDKK"})
            )
        else:
            p_h_hourly = pd.DataFrame(columns=["HourUTC", "PriceArea", "SpotPriceDKK"])

        p_combined = pd.concat([
            p_h[["HourUTC", "PriceArea", "SpotPriceDKK"]],
            p_h_hourly,
        ], ignore_index=True).sort_values(["HourUTC", "PriceArea"])

        q_h_EA = df_prod[["HourUTC", "PriceArea", "GrossConsumptionMWh"]].copy()

        p_combined = p_combined.merge(
            q_h_EA[["HourUTC", "PriceArea", "GrossConsumptionMWh"]],
            on=["HourUTC", "PriceArea"],
            how="left",
        )

        p_area = (
            p_combined[["HourUTC", "PriceArea", "SpotPriceDKK"]]
            .pivot_table(index="HourUTC", columns="PriceArea", values="SpotPriceDKK")
            .reset_index()
        )
        p_area.columns.name = None

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

        if save_txt:
            write_ep_txt(
                values     = wp_h["SpotPriceDKK_weighted"],
                timestamps = wp_h["HourUTC"],
                filename   = f"wp_{start}_{end}.txt",
                weights    = q_h["GrossConsumptionMWh"],
                EP_style   = EP_style,
            )

        results["wp_h"]   = wp_h
        results["q_h"]    = q_h
        results["p_area"] = p_area

    return results
