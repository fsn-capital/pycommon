from gcs.gcs import GCSHandler

with GCSHandler(project="fsn-insight", bucket_name="aztec-dev", use_backoff=True) as gcs_handler:
    for blob in gcs_handler.filter_updated(gcs_handler.list_blobs(), '1985-04-12T23:20:50.52+00:00'):
    # for blob in gcs_handler.list_blobs():
        print(blob.name, blob.updated)
        # stream = gcs_handler.download_blob_into_memory(blob.name)
        # break

