
import curses
import time

from .metrics import PerformanceMetrics


def run_dashboard(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(200)

    metrics = PerformanceMetrics()

    while True:
        key = stdscr.getch()

        if key in (ord("q"), ord("Q")):
            break

        start_ns = time.perf_counter_ns()

        # Temporary demo event; real engine integration comes later.
        time.sleep(0.001)

        end_ns = time.perf_counter_ns()
        metrics.record_latency(start_ns, end_ns)
        summary = metrics.get_summary()

        stdscr.erase()
        stdscr.addstr(0, 2, "CHRONOSMATCH - PERFORMANCE DASHBOARD")
        stdscr.addstr(2, 2, "Mode: Demo (simulated processing)")
        stdscr.addstr(4, 2, f"Orders processed: {summary['processed_orders']}")
        stdscr.addstr(
            5, 2,
            f"Average latency: {summary['average_latency_us']:.2f} microseconds"
        )
        stdscr.addstr(
            6, 2,
            f"Throughput: {summary['throughput_ops']:.2f} orders/second"
        )
        stdscr.addstr(9, 2, "Press Q to exit")
        stdscr.refresh()


if __name__ == "__main__":
    curses.wrapper(run_dashboard)
