import tempfile

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

        if self.confs_path is None:

            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile('w') as jsonfile:
                json.dump(json_keyfile_dict, jsonfile)
                jsonfile.flush()

                self.service = storage.Client.from_service_account_json(jsonfile.name)
        else:
            self.service = storage.Client.from_service_account_json(self.confs_path)
        self.logger = logger if logger is not None else EmptyLogger()

    def pd_to_gstorage(self, df, bucket_name, file_name_path):
        """
        :param df: pandas DataFrame to be saved on GCS
        :param bucket_name: GCS bucket name
        :param file_name_path: path to save file on bucket
        :return: True, False whether file is correctly saved or not
        """
        bucket = self.service.get_bucket(bucket_name)
        with tempfile.NamedTemporaryFile('w') as temp:
            df.to_parquet(temp.name + '.parquet', index=False)
            bucket.blob(file_name_path).upload_from_filename(temp.name + '.parquet', content_type='application/octet-stream')
            temp.flush()
            os.remove(temp.name + '.parquet')
            return True
        return False

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

