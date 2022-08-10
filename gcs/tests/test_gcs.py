import backoff
from gcs import __version__, Bucket, Client, Blob
from backoff import full_jitter
import unittest


class GCSTests(unittest.TestCase):

    def test_version(self):
        self.assertEqual(__version__, "0.1.0")

    def test_backoff_params(self):
        client = Client()
        self.assertIsNone(client.backoff_params)
        bucket = Bucket(client=client)
        self.assertIsNone(bucket.backoff_params)
        blob = Blob(name="test", bucket=bucket)
        self.assertIsNone(blob.backoff_params)

        client = Client(use_backoff=True)
        self.assertDictEqual(client.backoff_params, {
            "max_tries": 8,
            "jitter": full_jitter
        })
        bucket = Bucket(client=client, use_backoff=True)
        self.assertDictEqual(bucket.backoff_params, {
            "max_tries": 8,
            "jitter": full_jitter
        })
        blob = Blob(name="test", bucket=bucket, use_backoff=True)
        self.assertDictEqual(blob.backoff_params, {
            "max_tries": 8,
            "jitter": full_jitter
        })

        client = Client(backoff_params={
            "max_tries": 10,
            "jitter": None
        })
        self.assertDictEqual(client.backoff_params, {
            "max_tries": 10,
            "jitter": None
        })
        bucket = Bucket(client=client, backoff_params={
            "max_tries": 10,
            "jitter": None
        })
        self.assertDictEqual(bucket.backoff_params, {
            "max_tries": 10,
            "jitter": None
        })
        blob = Blob(name="test", bucket=bucket, backoff_params={
            "max_tries": 10,
            "jitter": None
        })
        self.assertDictEqual(blob.backoff_params, {
            "max_tries": 10,
            "jitter": None
        })

    def test_ratelimit_params(self):
        client = Client()
        self.assertIsNone(client.ratelimit_params)
        bucket = Bucket(client=client)
        self.assertIsNone(bucket.ratelimit_params)
        blob = Blob(name="test", bucket=bucket)
        self.assertIsNone(blob.ratelimit_params)

        client = Client(use_ratelimit=True)
        self.assertDictEqual(client.ratelimit_params, {
            "calls": 15,
            "period": 900
        })
        bucket = Bucket(client=client, use_ratelimit=True)
        self.assertDictEqual(bucket.ratelimit_params, {
            "calls": 15,
            "period": 900
        })
        blob = Blob(name="test", bucket=bucket, use_ratelimit=True)
        self.assertDictEqual(blob.ratelimit_params, {
            "calls": 15,
            "period": 900
        })

        client = Client(ratelimit_params={
            "calls": 1000,
            "period": 60
        })
        self.assertDictEqual(client.ratelimit_params, {
            "calls": 1000,
            "period": 60
        })
        bucket = Bucket(client=client, ratelimit_params={
            "calls": 1000,
            "period": 60
        })
        self.assertDictEqual(bucket.ratelimit_params, {
            "calls": 1000,
            "period": 60
        })
        blob = Blob(name="test", bucket=bucket, ratelimit_params={
            "calls": 1000,
            "period": 60
        })
        self.assertDictEqual(blob.ratelimit_params, {
            "calls": 1000,
            "period": 60
        })

if __name__ == "__main__":
    unittest.main()
