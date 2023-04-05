from __future__ import annotations
from google.cloud import bigquery  # type: ignore
from typing import Iterable, Mapping, Optional, Union, Type
from google.auth import credentials, default, exceptions as auth_exceptions  # type: ignore
from google.api_core.client_options import ClientOptions  # type: ignore
import requests.exceptions as requests_exceptions
from ratelimiter import RateLimiter
from retry import SimpleRetrier, backoff_hdlr, expo, full_jitter
import logging
from types import TracebackType
from traceback import print_exception
from google.api_core import exceptions
import re


_RETRYABLE_REASONS = frozenset(
    ["rateLimitExceeded", "backendError", "internalError", "badGateway"]
)

_UNSTRUCTURED_RETRYABLE_TYPES = (
    ConnectionError,
    exceptions.TooManyRequests,
    exceptions.InternalServerError,
    exceptions.BadGateway,
    requests_exceptions.ChunkedEncodingError,
    requests_exceptions.ConnectionError,
    requests_exceptions.Timeout,
    auth_exceptions.TransportError,
)

logger = logging.getLogger(__name__)


def _should_retry(exc):
    """Predicate for determining when to retry.

    We retry if and only if the 'reason' is 'backendError'
    or 'rateLimitExceeded'.
    """
    if not hasattr(exc, "errors") or len(exc.errors) == 0:
        # Check for unstructured error returns, e.g. from GFE
        return isinstance(exc, _UNSTRUCTURED_RETRYABLE_TYPES)

    reason = exc.errors[0]["reason"]
    return reason in _RETRYABLE_REASONS


class BQHandler:
    _default_backoff_params = {
        "kind": "on_predicate",
        "wait_gen": expo,
        "predicate": _should_retry,
        "on_backoff": backoff_hdlr,
        "max_tries": 8,
        "jitter": full_jitter,
        "max_time": 600,
    }
    _default_ratelimit_params = {"calls": 15, "period": 900}

    def __init__(
        self,
        project: str,
        credentials: Optional[credentials.Credentials] = None,
        api_endpoint: Optional[str] = None,
        **kwargs,
    ):
        use_backoff = kwargs.pop("use_backoff", False)
        self._backoff_params = kwargs.pop("backoff_params", None)

        if use_backoff or self._backoff_params:
            self._backoff_params = self._backoff_params or self._default_backoff_params

        use_ratelimit = kwargs.pop("use_ratelimit", False)
        self._ratelimit_params = kwargs.pop("ratelimit_params", None)
        self._ratelimiter = None
        if use_ratelimit or self._ratelimit_params:
            self._ratelimit_params = (
                self._ratelimit_params or self._default_ratelimit_params
            )
            self._ratelimiter = RateLimiter(**self._ratelimit_params)

        options = ClientOptions(api_endpoint=api_endpoint) if api_endpoint else None
        if options and not credentials:
            credentials, _ = default()
        self._client = bigquery.Client(
            project=project, credentials=credentials, client_options=options
        )

    @property
    def ratelimit_params(self) -> Mapping:
        return self._ratelimit_params

    @property
    def backoff_params(self) -> Mapping:
        return self._backoff_params

    def __enter__(self) -> BQHandler:
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        if self._ratelimiter:
            logger.debug("finalizing ratelimiter")
            self._ratelimiter.finalize()
        if not type and not value and not traceback:
            return True
        print_exception(type, value, traceback)
        return False

    @staticmethod
    def generate_load_job_config(**kwargs) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(**kwargs)

    @staticmethod
    def generate_extract_job_config(**kwargs) -> bigquery.ExtractJobConfig:
        return bigquery.ExtractJobConfig(**kwargs)

    @staticmethod
    def generate_gcs_uri(bucket_name: str, blob_name: str) -> str:
        return f"gs://{bucket_name}/{blob_name}"

    @staticmethod
    def generate_table_id(project: str, dataset_id: str, table_name: str) -> str:
        return f"{project}.{dataset_id}.{table_name}"

    def start_load_job(
        self, uri: str, table_id: str, job_config: Optional[bigquery.LoadJobConfig]
    ) -> bigquery.LoadJob:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def wait_for_job_completion(
        self, job: bigquery.LoadJob, timeout: Optional[float] = None
    ):
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        job.result(
            timeout=timeout,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def get_table(self, table_id: str) -> bigquery.Table:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.get_table(
            table_id,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def extract_table(
        self,
        table_ref: bigquery.TableReference,
        destination_uri: str,
        location: str,
        job_config: bigquery.ExtractJobConfig,
    ) -> bigquery.ExtractJob:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.extract_table(
            table_ref,
            destination_uri,
            location=location,
            job_config=job_config,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def get_job(
        self, job_id: str, location: str
    ) -> Union[
        bigquery.CopyJob,
        bigquery.LoadJob,
        bigquery.QueryJob,
        bigquery.ExtractJob,
        bigquery.UnknownJob,
    ]:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.get_job(
            job_id,
            location,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def cancel_job(
        self, job_id: str, location: str
    ) -> Union[
        bigquery.CopyJob,
        bigquery.LoadJob,
        bigquery.QueryJob,
        bigquery.ExtractJob,
    ]:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.cancel_job(
            job_id,
            location,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def list_datasets(self) -> Iterable:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.list_datasets(
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None
        )

    def list_tables(self, dataset_id: str) -> Iterable:
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        return self._client.list_tables(
            dataset_id,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
        )

    def delete_table(self, table_id: str, not_found_ok: bool = False):
        if self._ratelimit_params:
            self._ratelimiter.limit()  # type:ignore
        self._client.delete_table(
            table_id,
            retry=SimpleRetrier(**self._backoff_params)  # type: ignore
            if self._backoff_params
            else None,
            not_found_ok=not_found_ok,
        )

    def list_tables_matching_regex(self, dataset_id: str, pattern: str) -> Iterable:
        return filter(
            lambda x: re.match(pattern, x) is not None, self.list_tables(dataset_id)
        )
