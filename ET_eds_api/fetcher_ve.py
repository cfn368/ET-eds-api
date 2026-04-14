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
def VE(value_columns, cap_column, col_name, start, end, verbose=True, no_index=False, cap_ref=None, cache=False, cache_dir="eds_cache", save_txt=False):

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
        )

    return df_ve
