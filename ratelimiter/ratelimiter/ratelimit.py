from __future__ import annotations
import sys
from math import floor
from threading import RLock, Event, Thread, Condition
import logging
from traceback import print_exception
from types import TracebackType
from typing import Optional, Type, Union


logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, calls: int, period: Union[int, float]):
        self._max_calls = max(1, min(sys.maxsize, floor(calls)))
        self._period = period
        self._num_calls = 0
        self._lock = RLock()
        self._ticker = Event()
        self._resetter = Thread(target=self._reset)
        self._resetter.start()
        self._cv = Condition(lock=self._lock)

    def finalize(self):
        self._ticker.set()
        self._resetter.join()

    def __enter__(self) -> RateLimiter:
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        logger.debug("finalizing ratelimiter")
        self.finalize()
        if not type and not value and not traceback:
            return True
        print_exception(type, value, traceback)
        return False

    def _reset(self):
        def do_reset():
            logger.debug("resetting number of calls")
            with self._lock:
                self._num_calls = 0

        while not self._ticker.wait(self._period):
            do_reset()
            logger.debug("notify waiting threads")
            with self._cv:
                # n = max calls avoids thundering herd problem
                self._cv.notify(n=self._max_calls)

    def limit(self):
        if self._num_calls == self._max_calls:
            with self._cv:
                self._cv.wait()
        with self._lock:
            logger.debug("increase call counter")
            self._num_calls += 1
            logger.debug(f"num calls: {self._num_calls} max calls: {self._max_calls}")
