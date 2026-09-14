"""S3 object access and immutable control records. Never log credentials."""
import hashlib
import json
import os
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def settings():
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    if '://' not in endpoint:
        endpoint = 'http://' + endpoint
    return dict(endpoint=endpoint, bucket=os.getenv('MINIO_BUCKET', 'data-lake'),
                access=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
                secret=os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin'),
                region=os.getenv('AWS_REGION', 'us-west-2'))


class Objects:
    def __init__(self):
        self.settings = settings()
        s = self.settings
        self.bucket = s['bucket']
        self.client = boto3.client('s3', endpoint_url=s['endpoint'],
            aws_access_key_id=s['access'], aws_secret_access_key=s['secret'],
            region_name=s['region'], config=Config(signature_version='s3v4',
            s3={'addressing_style': 'path'}, retries={'max_attempts': 4}))
        self.client.head_bucket(Bucket=self.bucket)
    def get(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            with response['Body'] as stream:
                return stream.read()
        except ClientError as exc:
            if exc.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise
    def json(self, key):
        value = self.get(key)
        return json.loads(value) if value is not None else None
    def put(self, key, data, immutable=False):
        digest = hashlib.sha256(data).hexdigest()
        args = dict(Bucket=self.bucket, Key=key, Body=data,
                    Metadata={'sha256': digest})
        if immutable:
            args['IfNoneMatch'] = '*'
        try:
            self.client.put_object(**args)
        except ClientError as exc:
            if not immutable or exc.response['Error']['Code'] not in ('PreconditionFailed', '412', 'ConditionalRequestConflict'):
                raise
            if self.get(key) != data:
                raise ValueError(f'Immutable object conflict: {key}') from exc
        return dict(key=key, bytes=len(data), sha256=digest)
    def put_json(self, key, value, immutable=False):
        return self.put(key, json.dumps(value, sort_keys=True, ensure_ascii=False).encode(), immutable)
    def list(self, prefix):
        pages = self.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=prefix)
        return sorted(obj['Key'] for page in pages for obj in page.get('Contents', []))
    def arrow_fs(self):
        import pyarrow.fs as fs
        s = self.settings
        endpoint = urlparse(s['endpoint'])
        return fs.S3FileSystem(access_key=s['access'], secret_key=s['secret'],
            region=s['region'], endpoint_override=endpoint.netloc, scheme=endpoint.scheme)
