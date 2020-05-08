
import os

import pandas_gbq
from google.cloud.bigquery_storage_v1beta1 import BigQueryStorageClient
from google.oauth2 import service_account
from google.cloud import bigquery


class BigQueryConnector:
    def __init__(self, project_id, confs_path=None, auth_type='service_account', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type

        # authorization boilerplate code

        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        if self.json_keyfile_dict is None:
            self.creds = service_account.Credentials.from_service_account_file(
                self.confs_path,
            )
        else:
            self.creds = service_account.Credentials.from_service_account_info(
                self.json_keyfile_dict
            )
        self.project_id = project_id

        self.service = bigquery.Client(project=self.project_id, credentials=self.creds)

    def pd_execute(self, query, progress_bar_type=None, bqstorage_enabled=False, deduplicate_objects=True,
                   strings_to_categorical=True):
        """

        :param query:
        :param progress_bar_type:
        :return:
        """

        # Use a BigQuery Storage API client to download results more quickly.
        if bqstorage_enabled is True:
            bqstorage_client = BigQueryStorageClient(
                credentials=self.creds
            )
            return self.service.query(query).to_arrow(bqstorage_client=bqstorage_client,
                                                      progress_bar_type=progress_bar_type
                                                      ).to_pandas(deduplicate_objects=deduplicate_objects,
                                                                  strings_to_categorical=strings_to_categorical)
        else:
            return self.service.query(query).to_dataframe(progress_bar_type=progress_bar_type)
        # return pandas_gbq.read_gbq(query, project_id=self.project_id, credentials=self.creds, progress_bar_type=progress_bar_type, use_bqstorage_api=True)


