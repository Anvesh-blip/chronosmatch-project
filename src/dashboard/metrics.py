
from time import perf_counter_ns
from statistics import mean


class PerformanceMetrics:
    def __init__(self):
        self.latencies_us = []
        self.processed_orders = 0
        self.start_time = perf_counter_ns()

    def record_latency(self, start_ns, end_ns):
        if end_ns < start_ns:
            raise ValueError("End time cannot be before start time")

        latency_us = (end_ns - start_ns) / 1000
        self.latencies_us.append(latency_us)
        self.processed_orders += 1
        return latency_us

    def get_summary(self):
        elapsed = (perf_counter_ns() - self.start_time) / 1_000_000_000

        if not self.latencies_us:
            return {
                "processed_orders": 0,
                "average_latency_us": 0.0,
                "throughput_ops": 0.0,
            }

        return {
            "processed_orders": self.processed_orders,
            "average_latency_us": mean(self.latencies_us),
            "throughput_ops": (
                self.processed_orders / elapsed if elapsed > 0 else 0.0
            ),
        }
