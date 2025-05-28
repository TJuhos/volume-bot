"""Command-line entrypoint for running the volume bot."""

import argparse
import asyncio
from pathlib import Path

from bot import __version__, load_config
from bot.utils.logger import setup_logger  # to be provided in utils/
from bot.connectors.binance import BinanceConnector  # to be provided in connectors/
from bot.execution.order_manager import OrderManager  # to be provided in execution/


async def run_bot(config: dict) -> None:
    """Instantiate subsystems and start the trading loop."""
    logger = setup_logger(config["logging"]["level"])
    logger.info("Launching Volume Bot v%s", __version__)

    
    connector = BinanceConnector(config["exchange"], logger=logger)
    order_manager = OrderManager(connector=connector, config=config, logger=logger)

    await order_manager.run()  # placeholder coroutine


def cli() -> None:
    parser = argparse.ArgumentParser(description="Volume-providing market-making bot")
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path(__file__).with_name("config.yaml"),
        help="Path to the YAML configuration file",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    asyncio.run(run_bot(config))


if __name__ == "__main__":
    cli()
