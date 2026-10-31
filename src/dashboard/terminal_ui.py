
"""Windows-compatible ChronosMatch performance dashboard."""


def run_dashboard(summary=None):
    """Display actual simulation metrics in the terminal."""

    if summary is None:
        summary = {}

    print("\n" + "=" * 55)
    print("       CHRONOSMATCH - PERFORMANCE DASHBOARD")
    print("=" * 55)

    print("\nMode: Multiprocess Simulation Results")
    print(f"\nOrders processed: {summary.get('processed_orders', 0)}")

    print(
        "Average matching latency: "
        f"{summary.get('average_latency_us', 0.0):.2f} microseconds"
    )

    print(
        "Matching throughput: "
        f"{summary.get('throughput_ops', 0.0):.2f} orders/second"
    )

    print("\n" + "=" * 55)
    print("Dashboard completed.")
    print("=" * 55)


if __name__ == "__main__":
    run_dashboard()
