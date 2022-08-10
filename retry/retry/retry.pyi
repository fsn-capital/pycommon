from _typeshed import Incomplete
from ast import Call as Call
from typing import Callable, Optional

logger: Incomplete

def backoff_hdlr(details) -> None: ...

class SimpleRetrier:
    def __init__(self, *args, **kwargs) -> None: ...
    def __call__(self, fn: Callable) -> Callable: ...

class ConditionalRetrier(SimpleRetrier):
    def __init__(self, conditional_predicate, required_kwargs, *args, **kwargs) -> None: ...
    def get_retry_policy_if_conditions_met(self, **kwargs) -> Optional[Callable]: ...
