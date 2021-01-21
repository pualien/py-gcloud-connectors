import tempfile

import google
from retry import retry

from gcloud_connectors.logger import EmptyLogger
from google.cloud import storage
import os
import json


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

    def pd_to_gstorage(self, df, bucket_name, file_name_path):
        """
        :param df: pandas DataFrame to be saved on GCS
        :param bucket_name: GCS bucket name
        :param file_name_path: path to save file on bucket
        :return: True or error whether file is correctly saved or not
        """
        bucket = self.service.get_bucket(bucket_name)
        with tempfile.NamedTemporaryFile('w') as temp:
            df.to_parquet(temp.name + '.parquet', index=False)
            bucket.blob(file_name_path).upload_from_filename(temp.name + '.parquet', content_type='application/octet-stream')
            temp.flush()
            os.remove(temp.name + '.parquet')
            return True

    def recursive_delete(self, bucket_name, directory_path_to_delete):
        """
        :param bucket_name: GCS bucket name
        :param directory_path_to_delete: path to start recursive deletion
        :return: list of deleted files from GSC
        """
        bucket = self.service.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=directory_path_to_delete)
        deleted_files = []
        for blob in blobs:
            blob.delete()
            self.logger.info('deleted {}'.format(blob.name))
            deleted_files.append(blob.name)
        return deleted_files

    @retry((google.api_core.exceptions.GatewayTimeout), tries=3, delay=2)
    def copy_blob(self, source_bucket, dest_bucket, blob):
        return source_bucket.copy_blob(blob, dest_bucket, new_name=blob.name)

    def recursive_copy_between_buckets(self, source_bucket, dest_bucket, prefix, delimiter='/', to_delete=False):
        """

        :param source_bucket: source bucket where files are currently located
        :param dest_bucket: destination bucket where to copy files
        :param prefix: to filter based on path hierarchy
        :param delimiter: wildcard to match files
        :param to_delete: True if you want to delete blobs from source bucket, default is False
        :return:
        """
        source_bucket = self.service.get_bucket(source_bucket)
        dest_bucket = self.service.get_bucket(dest_bucket)

        blobs = source_bucket.list_blobs(prefix=prefix, delimiter=delimiter)  # assuming this is tested

        for blob in blobs:
            self.copy_blob(source_bucket=source_bucket, dest_bucket=dest_bucket, blob=blob)
            self.logger.info('copied {} from {} to {}'.format(blob.name, source_bucket.name, dest_bucket.name))
            if to_delete is True:
                source_bucket.delete_blob(blob.name)

