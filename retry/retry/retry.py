from __future__ import annotations
import logging
from typing import Callable, Optional, Sequence
import backoff

logger = logging.getLogger(__name__)


def backoff_hdlr(details):
    logger.debug(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


class SimpleRetrier:
    def __init__(self, *args, **kwargs):
        self._maximum = kwargs.get("max_value")
        self._deadline = kwargs.get("max_time")
        self._initial = kwargs.pop("initial", 0)
        self._multiplier = kwargs.get("factor", 1)
        self._kind = kwargs.pop("kind", "on_exception")
        fn = getattr(backoff, self._kind)
        assert callable(fn), "Unknown retry method"
        self._impl = fn(*args, **kwargs)
        assert callable(self._impl), f"Failed to generate backoff.{self._kind}"

    def __call__(self, fn: Callable) -> Callable:
        fn.__name__ = "retriable_api_request"

        def fn_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as err:
                if self._kind == "on_exception":
                    raise err
                return err

        return self._impl(fn_wrapper)


class ConditionalRetrier(SimpleRetrier):
    def __init__(
        self,
        conditional_predicate: Callable,
        required_kwargs: Sequence,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._conditional_predicate = conditional_predicate
        self._required_kwargs = required_kwargs

    def get_retry_policy_if_conditions_met(self, **kwargs) -> Optional[Callable]:
        if self._conditional_predicate(*[kwargs[key] for key in self._required_kwargs]):
            return self
        return None
