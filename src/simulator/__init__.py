"""Market simulator module for ChronosMatch."""

__all__ = [
    "MarketSimulator",
    "SimulatorConfig",
    "SimulationMetrics",
    "OrderGenerator",
]


def __getattr__(name):
    if name in __all__:
        from src.simulator import market_simulator

        return getattr(market_simulator, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")