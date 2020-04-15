import tempfile

from google.cloud import storage
import os


class GStorageConnector:
    def __init__(self, confs_path, auth_type='service_account'):
        self.confs_path = confs_path
        self.auth_type = auth_type

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        self.service = storage.Client()

    def pd_to_gstorage(self, df, bucket_name='docsity-da-test-gsc-store-bucket', file_name_path='da_gsc_macro/lang=it/country=Italy/y=2019/data.parquet'):
        """

        :param df:
        :param bucket_name:
        :param file_name_path:
        :return:
        """
        bucket = self.service.get_bucket(bucket_name)
        with tempfile.NamedTemporaryFile(delete=True) as temp:
            df.to_parquet(temp.name + '.parquet', index=False)
            bucket.blob(file_name_path).upload_from_filename(temp.name + '.parquet', content_type='application/octet-stream')
            temp.close()
            return True
        return False

