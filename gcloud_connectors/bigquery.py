
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

    def pd_execute(self, query, progress_bar_type=None, bqstorage_enabled=False):
        """

        :param bqstorage_enabled:
        :param query:
        :param progress_bar_type:
        :return:
        """

        # Use a BigQuery Storage API client to download results more quickly.
        if bqstorage_enabled is True:
            bqstorage_client = BigQueryStorageClient(
                credentials=self.creds
            )
            return (
                self.service.query(query)
                    .result()
                    .to_dataframe(bqstorage_client=bqstorage_client)
            )
        else:
            return (
                self.service.query(query)
                    .result()
                    .to_dataframe()
            )


