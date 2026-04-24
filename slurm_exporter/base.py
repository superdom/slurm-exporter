import threading
import time
from typing import Any, Dict, Iterator

from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import Collector


class SlurmBaseCollector(Collector):
    """Thread-safe base class for Prometheus custom collectors.

    Background threads atomically replace snapshots via _update(new_data).
    Prometheus scrapes acquire a snapshot reference under lock in collect(),
    then yield metrics without holding the lock. This removes the non-atomic
    clear()+set() window.
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {}
        self._scrape_errors: int = 0
        self._scrape_duration_ms: float = 0.0

    def _update(self, new_data: Dict[str, Any]) -> None:
        """Called by background threads. Replacing the dict reference is atomic."""
        with self._lock:
            self._data = new_data

    def _snapshot(self) -> Dict[str, Any]:
        """Called by scrape threads. Only acquire the reference under lock."""
        with self._lock:
            return self._data

    def _record_error(self) -> None:
        self._scrape_errors += 1

    def _start_timer(self) -> float:
        return time.time()

    def _stop_timer(self, start: float) -> None:
        self._scrape_duration_ms = (time.time() - start) * 1000.0

    def _health_metrics(self, prefix: str) -> Iterator:
        """Yield common scrape health metrics."""
        c = CounterMetricFamily(
            f"{prefix}_scrape_error",
            f"Total {prefix} scrape errors",
        )
        c.add_metric([], self._scrape_errors)
        yield c

        g = GaugeMetricFamily(
            f"{prefix}_scrape_duration",
            f"Last {prefix} scrape duration in milliseconds",
        )
        g.add_metric([], self._scrape_duration_ms)
        yield g

    def describe(self) -> Iterator:
        return iter([])

    def collect(self) -> Iterator:
        raise NotImplementedError
