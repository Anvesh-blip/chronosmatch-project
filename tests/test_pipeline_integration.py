import subprocess
import sys
from pathlib import Path


def test_full_ipc_matching_pipeline():
    project_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "run_ipc.py"],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert result.returncode == 0, result.stderr

    output = result.stdout

    assert "Order ID: 2002" in output
    assert "Order ID: 2003" in output
    assert "Order ID: 2004" in output
    assert (
        "TRADE: {'buy_order_id': 2004, "
        "'sell_order_id': 2003, "
        "'price': 15100, "
        "'quantity': 5}"
    ) in output
    assert "Stop signal received. Consumer exiting." in output