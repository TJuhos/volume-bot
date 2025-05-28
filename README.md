# Volume Bot — Market‑Making Reference Implementation

A lean, async‑powered Python 3.11 project that provides continuous two‑sided quotes on Binance Spot (or any CEX via the abstract connector).

## Features
* **Tight spreads / adaptive skew** based on inventory.
* **Configurable risk caps** with kill‑switch logic.
* **Back‑testing harness** that replays Parquet depth & trade data.
* **Docker‑first** deployment; runs equally on x86 or ARM.

## Quickstart
```bash
# Clone & install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure API keys
cp .env.example .env  # then edit

# Run in paper mode (testnet)
python -m scripts.run_bot -c bot/config.yaml