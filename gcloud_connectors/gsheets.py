import gspread
import requests
from googleapiclient.discovery import build
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d
from gspread_pandas import Spread
from oauth2client.service_account import ServiceAccountCredentials
from retry import retry

SCOPES = ['https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']


class GSheetsConnector:
    def __init__(self, confs_path=None, auth_type='service_accounts', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type

        if self.json_keyfile_dict is None:
            self.creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.confs_path, scopes=SCOPES)
        else:
            self.creds = ServiceAccountCredentials.from_json_keyfile_dict(
                self.json_keyfile_dict, scopes=SCOPES)

        self.service = build('sheets', 'v4', credentials=self.creds, cache_discovery=False)
        self.gspread = gspread.authorize(self.creds)

    def get_sheet_id_by_name(self, spreadsheet, worksheet_name):
        for wk in spreadsheet.worksheets():
            if wk.title == worksheet_name:
                return wk.id

    def delete_cells(self, spreadsheet_key, worksheet_name, start_index=1, end_index=30, dimension='COLUMNS',
                     additional_base_sheet=False):
        """
        :param spreadsheet_key: id for Spreadsheet taken from URL
        :param worksheet_name: name as visibile in worksheet
        :param start_index:
        :param end_index:
        :param dimension:
        :param additional_base_sheet: whethere to clean also base sheet
        :return:
        """
        spreadsheet = self.gspread.open_by_key(spreadsheet_key)
        worksheets_to_delete = [worksheet_name, 'Foglio1'] if additional_base_sheet is True else [worksheet_name]
        for wk_name in worksheets_to_delete:
            delete_body = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": self.get_sheet_id_by_name(spreadsheet, wk_name),
                                "dimension": dimension,
                                "startIndex": start_index,
                                "endIndex": end_index
                            }
                        }
                    }
                ]
            }
            try:
                spreadsheet.batch_update(delete_body)
            except gspread.exceptions.APIError as e:
                if "Invalid requests[0].deleteDimension: Cannot delete a column that doesn't exist." in e.response.json().get(
                        'error', {}).get('message'):
                    pass
                else:
                    raise e

    @retry((requests.exceptions.ReadTimeout, gspread.exceptions.APIError), tries=3, delay=2)
    def pd_to_gsheet(self, df, spreadsheet_key, worksheet_name, value_input_option='USER_ENTERED', clean=True, use_df2gsprad=True):
        """
        :param df: pandas DataFrame
        :param spreadsheet_key: id for Spreadsheet taken from URL
        :param worksheet_name: name as visibile in worksheet
        :param value_input_option: 'USER_ENTERED' if scope is to maintain column types from pandas DataFrame
        :param clean: whether to clean or not
        :param use_df2gsprad:
        :return:
        """
        if use_df2gsprad:
            if clean is True:
                self.delete_cells(spreadsheet_key, worksheet_name)
            return d2g.upload(df, spreadsheet_key, worksheet_name, credentials=self.creds, row_names=False,
                              value_input_option=value_input_option, clean=clean)

        else:
            if clean is True:
                self.delete_cells(spreadsheet_key, worksheet_name)
            x = Spread(spreadsheet_key, worksheet_name, creds=self.creds, create_sheet=True)
            return x.df_to_sheet(df, index=False, sheet=worksheet_name, replace=clean)


    def pd_read_gsheet(self, spreadsheet_key, worksheet_name):
        """
        :param spreadsheet_key: id for Spreadsheet taken from URL
        :param worksheet_name: name as visibile in worksheet
        :return: pandas DataFrame from worksheet
        """

        return g2d.download(gfile=spreadsheet_key, wks_name=worksheet_name, col_names=True, credentials=self.creds)
