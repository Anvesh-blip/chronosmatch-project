
import unittest

from src.dashboard.metrics import PerformanceMetrics


class TestPerformanceMetrics(unittest.TestCase):

    def test_latency_conversion(self):
        metrics = PerformanceMetrics()

        latency = metrics.record_latency(1_000_000, 1_020_000)

        self.assertEqual(latency, 20.0)
        self.assertEqual(metrics.processed_orders, 1)

    def test_empty_summary(self):
        metrics = PerformanceMetrics()
        summary = metrics.get_summary()

        self.assertEqual(summary["processed_orders"], 0)
        self.assertEqual(summary["average_latency_us"], 0.0)


if __name__ == "__main__":
    unittest.main()
