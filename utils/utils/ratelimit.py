import sys
from math import floor
from threading import RLock, Event, Thread
from time import monotonic
import logging
from time import sleep

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, calls: int, period: int):
        self._calls = max(1, min(sys.maxsize, floor(calls)))
        self._period = period
        self._num_calls = 0
        self._last_reset = monotonic()
        self._lock = RLock()
        self._ticker = Event()
        self._resetter = Thread(target=self._reset)
        self._resetter.start()

    def finalize(self):
        self._ticker.set()
        self._resetter.join()

    def _period_remaining(self) -> float:
        elapsed = monotonic() - self._last_reset
        return self._period - elapsed

    def _reset(self):
        def do_reset():
            logger.debug("resetting number of calls")
            with self._lock:
                self._num_calls = 0
                self._last_reset = monotonic()

        while not self._ticker.wait(self._period):
            do_reset()

    def limit(self):
        with self._lock:
            logger.debug("increase call counter")
            self._num_calls += 1
        if self._num_calls > self._calls:
            sleep(self._period_remaining())
