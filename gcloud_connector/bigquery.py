
import os

import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery


class BigQueryConnector:
    def __init__(self, confs_path, project_id, auth_type='service_account'):
        self.confs_path = confs_path
        self.auth_type = auth_type

        # authorization boilerplate code

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        self.creds = service_account.Credentials.from_service_account_file(
            confs_path,
        )
        self.project_id = project_id

        self.service = bigquery.Client(project=self.project_id)

    def pd_execute(self, query):
        """

        :param query:
        :return:
        """
        return pandas_gbq.read_gbq(query, project_id=self.project_id, credentials=self.creds)


