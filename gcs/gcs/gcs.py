from __future__ import annotations
from google.auth import credentials, default  # type: ignore
from google.cloud import storage  # type: ignore
from typing import Optional, Iterator, Union, Type, Mapping, cast
from datetime import datetime
import io
import logging
from google.cloud.storage.retry import (  # type: ignore
    is_generation_specified,
    is_etag_in_data,
    is_metageneration_specified,
    is_etag_in_json,
)
from traceback import print_exception
from ratelimiter import RateLimiter
from retry import SimpleRetrier, ConditionalRetrier, backoff_hdlr, expo, full_jitter
from types import TracebackType
from google.api_core import exceptions as api_exceptions
import requests
import requests.exceptions as requests_exceptions
from google.api_core.client_options import ClientOptions
from google.auth import exceptions as auth_exceptions


logger = logging.getLogger(__name__)

_RETRYABLE_TYPES = (
    api_exceptions.TooManyRequests,  # 429
    api_exceptions.InternalServerError,  # 500
    api_exceptions.BadGateway,  # 502
    api_exceptions.ServiceUnavailable,  # 503
    api_exceptions.GatewayTimeout,  # 504
    ConnectionError,
    requests.ConnectionError,
    requests_exceptions.ChunkedEncodingError,
    requests_exceptions.Timeout,
)

_ADDITIONAL_RETRYABLE_STATUS_CODES = (408,)


def _should_retry(exc):
    """Predicate for determining when to retry."""
    if isinstance(exc, _RETRYABLE_TYPES):
        return True
    elif isinstance(exc, api_exceptions.GoogleAPICallError):
        return exc.code in _ADDITIONAL_RETRYABLE_STATUS_CODES
    elif isinstance(exc, auth_exceptions.TransportError):
        return _should_retry(exc.args[0])
    else:
        return False


class GCSHandler:
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
        bucket_name: str,
        credentials: Optional[credentials.Credentials] = None,
        api_endpoint: Optional[str] = None,
        **kwargs,
    ):
        options = ClientOptions(api_endpoint=api_endpoint) if api_endpoint else None
        if options and not credentials:
            credentials, _ = default()
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
        self.client = storage.Client(
            project=project, credentials=credentials, client_options=options
        )
        self.bucket = self.client.bucket(bucket_name)

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

    def __enter__(self) -> GCSHandler:
        return self

    @property
    def backoff_params(self) -> Mapping:
        return self._backoff_params

    @property
    def ratelimit_params(self) -> Mapping:
        return self._ratelimit_params

    def list_blobs(self, prefix: Optional[str] = None) -> Iterator[storage.Blob]:
        if self._ratelimiter:
            self._ratelimiter.limit()
        return self.client.list_blobs(
            self.bucket,
            prefix=prefix,
            retry=SimpleRetrier(**self.backoff_params)
            if self._backoff_params
            else None,
        )

    def download_blob_into_memory(
        self, blob_name_or_blob: Union[str, storage.Blob]
    ) -> io.BytesIO:
        if self._ratelimiter:
            self._ratelimiter.limit()

        if isinstance(blob_name_or_blob, str):
            blob = self.bucket.blob(blob_name_or_blob)
            return self.download_blob_into_memory(blob)

        file_obj = io.BytesIO()
        blob_name_or_blob.download_to_file(
            file_obj,
            retry=SimpleRetrier(**self.backoff_params)
            if self._backoff_params
            else None,
        )
        file_obj.seek(0)
        return file_obj

    def upload_blob_from_memory(
        self, blob_name_or_blob: Union[str, storage.Blob], file_obj: io.BytesIO
    ):
        if self._ratelimiter:
            self._ratelimiter.limit()

        if isinstance(blob_name_or_blob, str):
            blob = self.bucket.blob(blob_name_or_blob)
            return self.upload_blob_from_memory(blob, file_obj)

        file_obj.seek(0)
        blob_name_or_blob.upload_from_file(
            file_obj,
            retry=ConditionalRetrier(
                is_generation_specified, ["query_params"], **self._backoff_params
            )
            if self._backoff_params
            else None,
        )

    @staticmethod
    def filter_updated(
        blobs: Iterator[storage.Blob], threshold: Union[str, datetime], cmp_op="gt"
    ):
        if isinstance(threshold, str):
            return GCSHandler.filter_updated(
                blobs, datetime.strptime(threshold, "%Y-%m-%dT%H:%M:%S.%f%z"), cmp_op
            )

        def resolve_op(
            lhs: storage.Blob, rhs: datetime = cast(datetime, threshold)
        ) -> bool:
            if hasattr(lhs, "updated"):
                if not lhs.updated:
                    return False
            else:
                return False
            if cmp_op == "gt":
                return lhs.updated > rhs
            elif cmp_op == "ge":
                return lhs.updated >= rhs
            elif cmp_op == "lt":
                return lhs.updated < rhs
            elif cmp_op == "le":
                return lhs.updated <= rhs
            elif cmp_op == "eq":
                return lhs.updated == rhs
            else:
                raise ValueError("Unknown comparison operator")

        return filter(resolve_op, blobs)
