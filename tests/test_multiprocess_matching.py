from src.ipc.multiprocess_runner import run_multiprocess_simulation


def test_multiprocess_pipeline_executes_matching():
    producer, consumer = run_multiprocess_simulation(
        total_orders=100,
        random_seed=42,
        timeout=30.0,
    )

    # Producer successfully submits every order.
    assert producer["status"] == "success"
    assert producer["orders_generated"] == 100
    assert producer["orders_submitted"] == 100
    assert producer["orders_failed"] == 0

    # Consumer receives every order in FIFO order.
    assert consumer["status"] == "success"
    assert consumer["orders_drained"] == 100
    assert consumer["first_order_id"] == 1001
    assert consumer["last_order_id"] == 1100
    assert consumer["fifo_violations"] == 0

    # Matching engine processes orders and executes trades.
    assert consumer["matching_metrics"]["processed_orders"] == 100
    assert consumer["trades_executed"] > 0
    assert consumer["total_traded_quantity"] > 0

    # Matching metrics are populated.
    assert consumer["matching_metrics"]["average_latency_us"] > 0
    assert consumer["matching_metrics"]["throughput_ops"] > 0
