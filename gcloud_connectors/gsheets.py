import gspread
import requests
from googleapiclient.discovery import build
from df2gspread import gspread2df as g2d
from gspread_pandas import Spread
from oauth2client.service_account import ServiceAccountCredentials
from retry import retry

SCOPES = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']


class GSheetsConnector:
    def __init__(self, confs_path, auth_type='service_account'):
        self.confs_path = confs_path
        self.auth_type = auth_type

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.confs_path, scopes=SCOPES)

        self.service = build('sheets', 'v4', credentials=creds)
        self.creds = creds

    @retry((requests.exceptions.ReadTimeout, gspread.exceptions.APIError), tries=3, delay=2)
    def pd_to_gsheet(self, df, spreadsheet_key, worksheet_name, value_input_option='USER_ENTERED', clean=True):
        """
        :param df:
        :param spreadsheet_key:
        :param worksheet_name:
        :return:
        """

        x = Spread(spreadsheet_key, worksheet_name, creds=self.creds, create_sheet=True)
        return x.df_to_sheet(df, index=False, sheet=worksheet_name, replace=clean)

    def get_sheet_id_by_name(self, spreadsheet, worksheet_name):
        for wk in spreadsheet.worksheets():
            if wk.title == worksheet_name:
                return wk.id

    def pd_read_gsheet(self, spreadsheet_key, worksheet_name):
        """
        :param spreadsheet_key:
        :param worksheet_name:
        :return:
        """

        return g2d.download(gfile=spreadsheet_key, wks_name=worksheet_name, col_names=True, credentials=self.creds)
