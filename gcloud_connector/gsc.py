from googleapiclient.discovery import build

from oauth2client.service_account import ServiceAccountCredentials
from retry import retry

from gcloud_connector.logger import EmptyLogger

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']


class GSCConnector():
    def __init__(self, confs_path, auth_type='service_accounts', logger=None):
        self.confs_path = confs_path
        self.auth_type = auth_type

        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.confs_path, scopes=SCOPES)

        self.service = build('webmasters', 'v3', credentials=self.creds)
        self.logger = logger if logger is not None else EmptyLogger()

    @retry(exceptions=Exception, tries=5)
    def execute_request(self, property_uri, request):
        """Executes a searchAnalytics.query request.
        Args:
          service: The webmasters service to use when executing the query.
          property_uri: The site or app URI to request data for.
          request: The request to be executed.
        Returns:
          An array of response rows.
        """

        return self.service.searchanalytics().query(
            siteUrl=property_uri, body=request).execute()

