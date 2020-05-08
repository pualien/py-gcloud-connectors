import tempfile

from google.oauth2 import service_account
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


    def pd_to_gstorage(self, df, bucket_name='docsity-da-test-gsc-store-bucket', file_name_path='da_gsc_macro/lang=it/country=Italy/y=2019/data.parquet'):
        """

        :param df:
        :param bucket_name:
        :param file_name_path:
        :return:
        """
        bucket = self.service.get_bucket(bucket_name)
        with tempfile.NamedTemporaryFile('w') as temp:
            df.to_parquet(temp.name + '.parquet', index=False)
            bucket.blob(file_name_path).upload_from_filename(temp.name + '.parquet', content_type='application/octet-stream')
            temp.flush()
            return True
        return False

