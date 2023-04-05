from bq import BQHandler
from retry import expo, full_jitter

PROJECT_ID = "<YOUR-PROJECT-ID-HERE>"
PRIVATE_CONNECTION_NAME = "<YOUR-PRIVATE-CONNECTION-NAME-HERE>"


def simple():
    with BQHandler(PROJECT_ID) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def simple_with_backoff():
    with BQHandler(PROJECT_ID, use_backoff=True) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def simple_with_ratelimit():
    with BQHandler("fsn-insight", use_ratelimit=True) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def backoff_on_exception():
    # full list of parameters
    # https://github.com/litl/backoff/blob/master/backoff/_decorator.py#L123

    backoff_params = {
        "kind": "on_exception",
        "wait_gen": expo,
        "exception": Exception,
        "max_tries": 3,
    }

    with BQHandler(PROJECT_ID, backoff_params=backoff_params) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def backoff_on_predicate():
    # full list of parameters
    # https://github.com/litl/backoff/blob/master/backoff/_decorator.py#L27

    def _should_retry(exc):
        return isinstance(exc, Exception)

    backoff_params = {
        "kind": "on_predicate",
        "wait_gen": expo,
        "jitter": full_jitter,
        "predicate": _should_retry,
        "max_time": 60,
    }

    with BQHandler(PROJECT_ID, backoff_params=backoff_params) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def custom_ratelimit():
    ratelimit_params = {
        "calls": 1,
        "period": 60,  # seconds
    }

    with BQHandler(PROJECT_ID, ratelimit_params=ratelimit_params) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def private_service_connect():
    api_endpoint = f"https://bigquery-{PRIVATE_CONNECTION_NAME}.p.googleapis.com"

    # this should fail outside of the VPC
    with BQHandler(PROJECT_ID, api_endpoint=api_endpoint) as bq:
        for dataset in bq.list_datasets():
            print(dataset.dataset_id)


def list_table():
    with BQHandler(PROJECT_ID) as bq:
        for table in bq.list_tables("PLACEHOLDER"):
            print(table.table_id)


def list_table_matching_regex():
    with BQHandler(PROJECT_ID) as bq:
        for table in bq.list_tables_matching_regex("PLACEHOLDER", "PLACEHOLDER"):
            print(table.table_id)


def remove_table():
    with BQHandler(PROJECT_ID) as bq:
        bq.delete_table(
            bq.generate_table_id(PROJECT_ID, "PLACEHOLDER", "PLACEHOLDER"),
            not_found_ok=False,
        )
