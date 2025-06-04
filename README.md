# Volume Bot — Grid Trading Reference Implementation

Volume Bot implements a lightweight grid trading strategy for Binance Spot (or any exchange that provides a compatible connector).  The `OrderManager` continuously keeps two orders in the book: a buy order below the current mid price and a sell order above it.  When one side fills the remaining order is cancelled and a new pair of orders is opened around the latest mid price.  The distance from the mid price is configured in basis points.

## Features
* **Grid trading core** – automatically re-quotes around the mid price.
* **Inventory & PnL tracking** – maintains balances and reports round trips.
* **Connector abstraction** – Binance implementation included; others can be added.
* **Back-test harness** – replay Parquet depth and trade data for research.
* **Docker-friendly** – can be deployed on both x86 and ARM.

## Quickstart
```bash
# Set up Python 3.10+ environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # or `poetry install`

# Configure credentials and trading parameters
cp .env.example .env  # edit with API keys
# edit bot/config.yaml with symbol, order_size and grid_spacing_bps

# Run against Binance (paper mode if configured)
python -m scripts.run_bot -c bot/config.yaml
```

For historical simulation:
```bash
python -m scripts.backtest --snapshot snapshots/FILE.parquet \
                           --diffs diffs/FILE.parquet \
                           --trades trades/FILE.parquet \
                           --config bot/config.yaml
```

## Project layout
- `bot/connectors` – exchange API connectors
- `bot/execution/order_manager.py` – core grid logic
- `bot/strategy` – helper classes for pricing and inventory
- `scripts` – entry points for live trading and backtesting

The order manager places a buy at `mid_price * (1 - grid_spacing_bps/10000)` and a sell at `mid_price * (1 + grid_spacing_bps/10000)`.  After any fill it cancels the opposite order and re-quotes around the fresh mid price.
