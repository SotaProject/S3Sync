import boto3
from tqdm import tqdm
import io
import logging
import os

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logging.info("Starting synchronization")


a_bucket = os.getenv('a_bucket')
a_aws_access_key_id = os.getenv('a_access_key_id')
a_aws_secret_access_key = os.getenv('a_access_key')
a_endpoint_url = os.getenv('a_endpoint')


a_s3_client = boto3.client(
    "s3",
    endpoint_url=a_endpoint_url,
    aws_access_key_id=a_aws_access_key_id,
    aws_secret_access_key=a_aws_secret_access_key,
    region_name="auto",
)


a_paginator = a_s3_client.get_paginator("list_objects_v2")
pages = a_paginator.paginate(Bucket=a_bucket)

logging.info("Getting keys from bucket A")
a_objs = []
for page in pages:
    for obj in page["Contents"]:
        a_objs.append(obj["Key"])
logging.info(f"{len(a_objs)} keys in bucket A")


b_bucket = os.getenv('b_bucket')
b_aws_access_key_id = os.getenv('b_access_key_id')
b_aws_secret_access_key = os.getenv('b_access_key')
b_endpoint_url = os.getenv('b_endpoint')

b_s3_client = boto3.client(
    "s3",
    endpoint_url=b_endpoint_url,
    aws_access_key_id=b_aws_access_key_id,
    aws_secret_access_key=b_aws_secret_access_key,
)


b_paginator = b_s3_client.get_paginator("list_objects_v2")
pages = b_paginator.paginate(Bucket=b_bucket)

logging.info("Getting keys from bucket B")
b_objs = []
for page in pages:
    for obj in page["Contents"]:
        b_objs.append(obj["Key"])
logging.info(f"{len(b_objs)} keys in bucket B")

b_set = set(b_objs)
sync_a2b = []
for a_obj in a_objs:
    if a_obj not in b_set:
        sync_a2b.append(a_obj)
logging.info(f"{len(sync_a2b)} keys from bucket A not in bucket B")

with tqdm(sync_a2b, desc="Syncing") as t:
    for obj in t:
        file_stream = io.BytesIO()
        t.set_description(f"Downloading {obj}")
        a_s3_client.download_fileobj(a_bucket, obj, file_stream)
        file_stream.seek(0)
        t.set_description(f"Uploading {obj}")
        b_s3_client.upload_fileobj(Fileobj=file_stream, Bucket=b_bucket, Key=obj)
logging.info("Finished")
