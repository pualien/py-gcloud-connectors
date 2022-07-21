import googleapiclient
import google.auth
from googleapiclient.discovery import build
import httplib2
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from retry import retry

from gcloud_connectors.logger import EmptyLogger

SCOPES = ['https://www.googleapis.com/auth/analytics']


class GAError(Exception):
    """Base class for GA exceptions"""


class GAPermissionError(GAError):
    """Base class for GA permission exceptions"""
    pass


class GAnalyticsConnector:
    def __init__(self, confs_path=None, auth_type='service_accounts', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type

        if self.json_keyfile_dict is not None or self.confs_path is not None:
            if self.json_keyfile_dict is None:
                self.creds = ServiceAccountCredentials.from_json_keyfile_name(
                    self.confs_path, scopes=SCOPES)
            else:
                self.creds = ServiceAccountCredentials.from_json_keyfile_dict(
                    self.json_keyfile_dict, scopes=SCOPES)
            self.http = self.creds.authorize(httplib2.Http())
            self.service = build('analytics', 'v4', http=self.http,
                                 discoveryServiceUrl='https://analyticsreporting.googleapis.com/$discovery/rest',
                                 cache_discovery=False)
        else:
            self.creds, project = google.auth.default()
            self.service = build('analytics', 'v4', credentials=self.creds,
                                 discoveryServiceUrl='https://analyticsreporting.googleapis.com/$discovery/rest',
                                 cache_discovery=False)


        self.logger = logger if logger is not None else EmptyLogger()
        self.management_service = None

    def get_segments_by_id(self, segment_id):
        if self.http:
            self.management_service = build('analytics', 'v3', http=self.http)
        else:
            self.management_service = build('analytics', 'v3', credentials=self.creds)
        segments = self.management_service.management().segments().list().execute().get('items', [])
        for segment in reversed(segments):
            pass

    @retry(googleapiclient.errors.HttpError, tries=3, delay=2)
    def pd_get_report(self, view_id, start_date, end_date, metrics, dimensions, filters=None, page_size=100000,
                      page_token=None, comes_from_sampling=False, segments=None, *args, **kwargs):
        """
        :param view_id: Google Analytics view id
        :param start_date: start_date to get data
        :param end_date: end_data to get data
        :param metrics: list of ga: metrics
        :param dimensions: list of ga: dimensions
        :param filters: filters in string type format. Ex. ga:deviceCategory==tablet
        :param page_size: max size of results per page
        :param page_token: token that identifies the page of results (depending on page_size)
        :param comes_from_sampling: to better debug unsampling capabilities, only with active logger initiated with GAnalyticsConnector
        :param segments: list of Google Analytics segments in format ['gaid::...', ...]
        :return: pandas DataFrame unsampled results from Google Analytics
        """
        self.logger.info('start view {view_id} from sampled data {comes_from_sampling} '
                         'from {start_date} to {end_date} '
                         'with metrics ({metrics}), dimensions ({dimensions}), filters ({filters}), page_token {page_token}'
                         .format(view_id=view_id, comes_from_sampling=comes_from_sampling,
                                 start_date=start_date, end_date=end_date, metrics=', '.join(metrics),
                                 dimensions=', '.join(dimensions),
                                 filters=filters, page_token=page_token))
        body = {
            'reportRequests': [
                {
                    'viewId': view_id,  # Add View ID from GA
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': [{'expression': metric} for metric in metrics],
                    'dimensions': [{'name': dimension} for dimension in dimensions],  # Get Pages
                    # 'orderBys': [{"fieldName": "ga:sessions", "sortOrder": "DESCENDING"}],
                    'pageSize': page_size
                }]
        }
        if filters is not None:
            body['reportRequests'][0]['filtersExpression'] = filters
        if page_token is not None:
            body['reportRequests'][0]['pageToken'] = page_token
        if segments is not None:
            body['reportRequests'][0]["segments"] = [{"segmentId": segment} for segment in segments] if not isinstance(
                segments[0], dict) else segments
            body['reportRequests'][0]["dimensions"].append({"name": "ga:segment"})

        response = self.service.reports().batchGet(body=body).execute()

        df = self.get_df_from_response(response, dimensions, metrics)
        if response['reports'][0]['data'].get('samplesReadCounts') is not None:
            self.logger.info('unsampling for {} {}'.format(start_date, end_date))
            # difference of start_date minus end_date in days, then split api of api calls in two parts:
            # start_date until half difference in days
            # half difference in days plus one day until end_date
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
            middle_dt = start_date_dt + (end_date_dt - start_date_dt) / 2
            if end_date_dt > start_date_dt:
                if middle_dt == end_date_dt:
                    middle_dt = start_date_dt
                df = self.pd_get_report(view_id, start_date_dt.strftime('%Y-%m-%d'), middle_dt.strftime('%Y-%m-%d'),
                                        metrics, dimensions,
                                        filters=filters, page_size=page_size, page_token=page_token, segments=segments,
                                        comes_from_sampling=True)

                middle_dt_plus_1 = (middle_dt + timedelta(days=1))
                middle_dt_plus_1 = middle_dt_plus_1 if middle_dt_plus_1 <= end_date_dt else end_date_dt
                df = df.append(
                    self.pd_get_report(view_id, middle_dt_plus_1.strftime('%Y-%m-%d'), end_date_dt.strftime('%Y-%m-%d'),
                                       metrics, dimensions,
                                       filters=filters, page_size=page_size, page_token=page_token, segments=segments,
                                       comes_from_sampling=True))

        else:
            self.logger.info('no sample for {} {}'.format(start_date, end_date))
            next_page_token = response['reports'][0].get('nextPageToken')
            if next_page_token is not None:
                df = df.append(self.pd_get_report(view_id, start_date, end_date, metrics, dimensions,
                                                  filters=filters, page_size=page_size, page_token=next_page_token,
                                                  segments=segments))

        return df

    @retry(googleapiclient.errors.HttpError, tries=3, delay=2)
    def pd_get_raw_report(self, report_request, dimensions, metrics, page_size=10000, page_token=None,
                          comes_from_sampling=False):
        """
        Useful if combined with Export Reporting API v4 taken from Da Vinci Tools Chrome Extension https://chrome.google.com/webstore/detail/da-vinci-tools/pekljbkpgnpphbkgjbfgiiclemodfpen
        :param report_request: accepts only one report request at time to unsample correctly
        :param page_size: max size of results per page
        :param page_token: token that identifies the page of results (depending on page_size)
        :param comes_from_sampling: to better debug unsampling capabilities, only with active logger initiated with GAnalyticsConnector
        :return: pandas DataFrame unsampled results from Google Analytics
        :return:
        """
        start_date = report_request['reportRequests'][0]['dateRanges'][0]['startDate']
        end_date = report_request['reportRequests'][0]['dateRanges'][0]['endDate']
        report_request['reportRequests'][0]['pageSize'] = page_size
        if page_token is not None:
            report_request['reportRequests'][0]['pageToken'] = page_token
        response = self.service.reports().batchGet(body=report_request).execute()

        df = self.get_df_from_response(response, dimensions, metrics)

        if response['reports'][0]['data'].get('samplesReadCounts') is not None:
            self.logger.info('unsampling for {} {}'.format(start_date, end_date))
            # difference of start_date minus end_date in days, then split api of api calls in two parts:
            # start_date until half difference in days
            # half difference in days plus one day until end_date
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
            middle_dt = start_date_dt + (end_date_dt - start_date_dt) / 2
            if end_date_dt > start_date_dt:
                if middle_dt == end_date_dt:
                    middle_dt = start_date_dt
                report_request['reportRequests'][0]['dateRanges'][0]['startDate'] = start_date_dt.strftime('%Y-%m-%d')
                report_request['reportRequests'][0]['dateRanges'][0]['endDate'] = middle_dt.strftime('%Y-%m-%d')
                df = self.pd_get_raw_report(report_request,
                                            metrics, dimensions,
                                            comes_from_sampling=True)

                middle_dt_plus_1 = (middle_dt + timedelta(days=1))
                middle_dt_plus_1 = middle_dt_plus_1 if middle_dt_plus_1 <= end_date_dt else end_date_dt
                report_request['reportRequests'][0]['dateRanges'][0]['startDate'] = middle_dt_plus_1.strftime(
                    '%Y-%m-%d')
                report_request['reportRequests'][0]['dateRanges'][0]['endDate'] = end_date_dt.strftime('%Y-%m-%d')
                df = df.append(
                    self.pd_get_raw_report(report_request,
                                           metrics, dimensions,
                                           comes_from_sampling=True))
            else:
                self.logger.info('no sample for {} {}'.format(start_date, end_date))
                next_page_token = response['reports'][0].get('nextPageToken')
                if next_page_token is not None:
                    df = df.append(self.pd_get_raw_report(report_request, dimensions, metrics, page_size=10000,
                                                          page_token=None, comes_from_sampling=False))

            return df
        return df

    @staticmethod
    def get_df_from_response(response, dimensions, metrics):
        """
        :param response: raw response from Google Analytics API
        :param dimensions: list of Google Analytics dimensions
        :param metrics: list of Google Analytics metrics
        :return: pandas DataFrame of results from response
        """
        data_dic = {f"{i}": [] for i in dimensions + metrics}
        for report in response.get('reports', []):
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                for i, key in enumerate(dimensions):
                    data_dic[key].append(row.get('dimensions', [])[i])  # Get dimensions
                date_range_values = row.get('metrics', [])
                for values in date_range_values:
                    all_values = values.get('values', [])  # Get metric values
                    for i, key in enumerate(metrics):
                        data_dic[key].append(all_values[i])

        df = pd.DataFrame(data=data_dic)
        df.columns = [col.split(':')[-1] for col in df.columns]

        return df
