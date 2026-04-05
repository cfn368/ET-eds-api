# ET-eds-api

Python client for the [Energi Data Service](https://www.energidataservice.dk/) API, focused on two things:

- **Spot prices** — consumption-weighted hourly prices for Denmark, stitched across the `Elspotprices` and `DayAheadPrices` datasets
- **Volume-equivalent production** — renewable production deflated by installed capacity, isolating the weather signal from capacity growth

## Install

```bash
pip install ET-eds-api
```

## Usage

### Prices

```python
from ET_eds_api import get_wp_h, wagg_wp

# Hourly consumption-weighted price, gross consumption, and per-area prices
wp_h, q_h, p_area = get_wp_h(start=2023, end=2025)
# p_area has columns: HourUTC | DK1 | DK2

# Aggregated to daily / weekly / monthly / yearly
wp_d, wp_w, wp_m, wp_y = wagg_wp(start=2023, end=2025)

# Cache responses to eds_cache/ — subsequent calls load from disk
wp_h, q_h, p_area = get_wp_h(start=2023, end=2025, cache=True)
```

### Volume-equivalent production

```python
from ET_eds_api import VE, columns

# See available column options
columns()

# Capacity-deflated solar production
solar_ve = VE(
    value_columns = ["SolarPowerLt10kW_MWh", "SolarPowerGe10Lt40kW_MWh", "SolarPowerGe40kW_MWh"],
    cap_column    = ["SolarPowerCapacity"],
    col_name      = "solar_VE",
    start         = 2022,
    end           = 2025,
    cache         = True,
)

# Counterfactual — normalise to a fixed capacity (e.g. 1 GW)
solar_ve_1gw = VE(
    ...,
    cap_ref = 1_000,
)
```

## Breaking changes

**0.1.5** — `get_wp_h` now returns three values instead of two. Update any existing unpacking:

```python
# before
wp_h, q_h = get_wp_h(...)

# after
wp_h, q_h, p_area = get_wp_h(...)
```

## Data sources

| Dataset | Used for |
|---|---|
| [Elspotprices](https://www.energidataservice.dk/tso-electricity/Elspotprices) | Spot prices up to ~2025-09-30 |
| [DayAheadPrices](https://www.energidataservice.dk/tso-electricity/DayAheadPrices) | Spot prices from ~2025-10-01 |
| [ProductionConsumptionSettlement](https://www.energidataservice.dk/tso-electricity/ProductionConsumptionSettlement) | Consumption weights + production |
| [CapacityPerMunicipality](https://www.energidataservice.dk/tso-electricity/CapacityPerMunicipality) | Installed capacity index |


---

Developed by [Linus Lindquist](https://github.com/cfn368) for [Erhvervslivets Tænketank](https://www.e-tank.dk) as part of Kernekraftprojektet.