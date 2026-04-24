import threading
import time
from typing import Any, Dict, Iterator

from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import Collector


class SlurmBaseCollector(Collector):
    """스레드 안전 Custom Collector 기반 클래스.

    배경 스레드가 _update(new_data)로 스냅샷을 원자적으로 교체하고,
    Prometheus 스크랩은 collect()에서 락 하에 스냅샷 참조를 획득한 뒤
    락 없이 메트릭을 yield한다. clear()+set() 비원자 구간을 원천 제거.
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {}
        self._scrape_errors: int = 0
        self._scrape_duration_ms: float = 0.0

    def _update(self, new_data: Dict[str, Any]) -> None:
        """배경 스레드에서 호출. dict 참조 교체는 원자적."""
        with self._lock:
            self._data = new_data

    def _snapshot(self) -> Dict[str, Any]:
        """스크랩 스레드에서 호출. 락 하에 참조만 획득."""
        with self._lock:
            return self._data

    def _record_error(self) -> None:
        self._scrape_errors += 1

    def _start_timer(self) -> float:
        return time.time()

    def _stop_timer(self, start: float) -> None:
        self._scrape_duration_ms = (time.time() - start) * 1000.0

    def _health_metrics(self, prefix: str) -> Iterator:
        """공통 scrape 헬스 메트릭 yield."""
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
