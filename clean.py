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

a_set = set(a_objs)
remove_b = []
for b_obj in b_objs:
    if b_obj not in a_set:
        remove_b.append(b_obj)

logging.info(f"{len(remove_b)} keys from bucket B to remove")

with tqdm(remove_b, desc="Deleting") as t:
    for obj in t:
        t.set_description(f"Deleting {obj}")
        b_s3_client.delete_object(Bucket=b_bucket, Key=obj)
        file_stream = io.BytesIO()
logging.info("Finished")
