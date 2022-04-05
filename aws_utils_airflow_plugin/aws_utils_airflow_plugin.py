"""Main module."""

from botocore.exceptions import ClientError, EndpointConnectionError
from jinja2 import Environment, BaseLoader
import boto3
import botocore
import time
from pprint import pprint
from urllib.parse import urlparse
from typing import List, Set, Dict, Tuple, Optional
import os
import fsspec
import s3fs


from airflow import DAG
from airflow.models import Connection
from airflow.utils.session import create_session
from airflow.utils.cli import suppress_logs_and_warning
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import fsspec

from aws_batch_helpers.aws_s3_helpers import S3Url

import logging
logging.basicConfig(level=logging.INFO)


class AwsExtendedHook(AwsBaseHook):

    def __init__(self, *args, **kwargs) -> None:
        kwargs["resource_type"] = "ec2"
        super().__init__(*args, **kwargs)

    def get_client(self):
        session = self.get_session()
        return session.client(self.resource_type)

    def get_aws_region(self):
        session = self.get_session()
        return session.region_name


class BatchHook(AwsExtendedHook):
    """
    Interact with AWS Batch Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.resource_type = "batch"


class S3Hook(AwsExtendedHook):
    """
    Interact with AWS S3 Storage.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.resource_type = "s3"

    @property
    def s3fs(self):
        credentials = self.get_credentials()
        return s3fs.S3FileSystem(
            key=credentials.access_key, secret=credentials.secret_key,
        )

    def bucket_exists(self, bucket_name):
        s3_session = self.get_session()
        s3_resource = s3_session.resource('s3')

        if 's3://' in str(bucket_name):
            s3_bucket_url = S3Url(str(bucket_name))
            bucket_name = s3_bucket_url.bucket

        bucket = s3_resource.Bucket(bucket_name)

        if bucket.creation_date:
            return True
        else:
            return False

    def key_exists(self, bucket_name, key_path):

        s3_session = self.get_session()
        s3_resource = s3_session.resource('s3')

        if 's3://' in str(bucket_name):
            s3_bucket_url = S3Url(str(bucket_name))
            bucket_name = s3_bucket_url.bucket

        if 's3://' in str(key_path):
            s3_bucket_url = S3Url(str(key_path))
            key_path = s3_bucket_url.key

        try:
            s3_resource.Object(bucket_name, key_path).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                return False
        return True

    def prefix_exists(self, bucket_name, prefix):
        s3_client = self.get_client()
        try:
            res = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        except Exception as e:
            return False

        if len(res['Contents']):
            return True
        else:
            return False


class LogsHook(AwsExtendedHook):
    """
    Interact with AWS Logs Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.resource_type = "logs"


def list_connections():
    conns = []
    try:
        with create_session() as session:
            query = session.query(Connection)
            query = query.filter(Connection.conn_type == "aws")
        for con in query.all():
            conns.append(con.__dict__)
    except Exception as e:
        logging.debug(e)
        conns = []
    return conns


def list_connections_as_wtforms_choices():
    choices = []
    conns = list_connections()

    for conn in conns:
        choices.append((conn["conn_id"], conn["conn_id"]))

    return choices
