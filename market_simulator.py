"""Top-level CLI entry point for the ChronosMatch Market Simulator.

Allows running:
    python market_simulator.py --orders 1000 --with-consumer
"""

import sys
from pathlib import Path

# Ensure root directory is on sys.path
root_dir = str(Path(__file__).resolve().parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from src.simulator.market_simulator import (
    MarketSimulator,
    OrderGenerator,
    SimulationMetrics,
    SimulatorConfig,
    main,
)

__all__ = [
    "MarketSimulator",
    "SimulatorConfig",
    "SimulationMetrics",
    "OrderGenerator",
    "main",
]

if __name__ == "__main__":
    main()
