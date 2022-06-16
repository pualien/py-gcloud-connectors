import json
import os
import socket
import tempfile
from operator import itemgetter

import google
import requests
import urllib3
from google.cloud import storage
from retry import retry

from gcloud_connectors.logger import EmptyLogger


class GStorageConnector:
    def __init__(self, confs_path=None, auth_type='service_account', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type
        # authorization boilerplate code
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        if self.json_keyfile_dict is not None:

            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile('w') as jsonfile:
                json.dump(json_keyfile_dict, jsonfile)
                jsonfile.flush()

                self.service = storage.Client.from_service_account_json(jsonfile.name)
        elif confs_path is not None:
            self.service = storage.Client.from_service_account_json(self.confs_path)
        else:
            self.service = storage.Client()
        self.logger = logger if logger is not None else EmptyLogger()

    @retry((socket.timeout, requests.exceptions.ConnectionError, urllib3.exceptions.ProtocolError), tries=3, delay=2)
    def pd_to_gstorage(self, df, bucket_name, file_name_path, tempfile_mode=True, partition_cols=None, **kwargs):
        """
        :param df: pandas DataFrame to be saved on GCS
        :param bucket_name: GCS bucket name
        :param file_name_path: path to save file on bucket
        :param tempfile_mode: if using a tempfile before pushing to GCS
        :param partition_cols: columns for partitioning in order of partitions
        :return: True or error whether file is correctly saved or not
        """
        if partition_cols is None:
            if tempfile_mode:
                bucket = self.service.get_bucket(bucket_name)
                with tempfile.NamedTemporaryFile('w') as temp:
                    df.to_parquet(temp.name + '.parquet', index=False)
                    bucket.blob(file_name_path).upload_from_filename(temp.name + '.parquet',
                                                                     content_type='application/octet-stream')
                    temp.flush()
                    os.remove(temp.name + '.parquet')
                    return True
            else:
                # only works for the following order: gcloud CLI default, gcsfs cached token,
                # google compute metadata service, anonymous
                df.to_parquet(
                    'gcs://{bucket_name}/{file_name_path}'.format(bucket_name=bucket_name, file_name_path=file_name_path),
                    index=False)
        else:
            # only works for the following order: gcloud CLI default, gcsfs cached token,
            # google compute metadata service, anonymous
            df.to_parquet(
                'gcs://{bucket_name}/{file_name_path}'.format(bucket_name=bucket_name, file_name_path=file_name_path),
                index=False, partition_cols=partition_cols, **kwargs)

    def recursive_delete(self, bucket_name, directory_path_to_delete):
        """
        :param bucket_name: GCS bucket name
        :param directory_path_to_delete: path to start recursive deletion
        :return: list of deleted files from GSC
        """
        bucket = self.service.get_bucket(bucket_name)
        blobs = self.service.list_blobs(bucket.name, prefix=directory_path_to_delete)
        deleted_files = []
        for blob in blobs:
            blob.delete()
            self.logger.info('deleted {}'.format(blob.name))
            deleted_files.append(blob.name)
        return deleted_files

    @retry((google.api_core.exceptions.GatewayTimeout), tries=3, delay=2)
    def copy_blob(self, source_bucket, dest_bucket, blob):
        return source_bucket.copy_blob(blob, dest_bucket, new_name=blob.name)

    def rename_blob(self, bucket_name, blob_name, new_name):
        """
        :param bucket_name: bucket
        :param blob_name: current blob name
        :param new_name: new blob name
        :return: new blob object
        """

        bucket = self.service.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        new_blob = bucket.rename_blob(blob, new_name)
        return new_blob

    def recursive_copy_between_buckets(self, source_bucket, dest_bucket, prefix, delimiter='/', to_delete=False,
                                       reverse_order=False):
        """
        :param source_bucket: source bucket where files are currently located
        :param dest_bucket: destination bucket where to copy files
        :param prefix: to filter based on path hierarchy
        :param delimiter: wildcard to match files
        :param to_delete: True if you want to delete blobs from source bucket, default is False
        :param reverse_order: True if you want to revert order starting from last added objects
        :return:
        """
        source_bucket = self.service.get_bucket(source_bucket)
        dest_bucket = self.service.get_bucket(dest_bucket)

        if reverse_order:
            unordered_blobs = self.service.list_blobs(source_bucket.name, prefix=prefix, delimiter=delimiter)
            blobs = []
            for blob in unordered_blobs:
                blobs.append((blob, blob.name))
            blobs.sort(key=itemgetter(1), reverse=True)
            blobs = [x[0] for x in blobs]
        else:
            blobs = self.service.list_blobs(source_bucket.name, prefix=prefix, delimiter=delimiter)

        for blob in blobs:
            self.copy_blob(source_bucket=source_bucket, dest_bucket=dest_bucket, blob=blob)
            self.logger.info('copied {} from {} to {}'.format(blob.name, source_bucket.name, dest_bucket.name))
            if to_delete is True:
                source_bucket.delete_blob(blob.name)
