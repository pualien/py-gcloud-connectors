from __future__ import print_function

import io
import pickle
import os.path

from gcloud_connectors.logger import EmptyLogger
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file']


class GDriveConnector:
    def __init__(self, confs_path=None, auth_type='service_accounts', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.auth_type = auth_type
        self.logger = logger if logger is not None else EmptyLogger()

        if self.auth_type == 'service_accounts':
            self.confs_path = confs_path
            self.json_keyfile_dict = json_keyfile_dict
            self.auth_type = auth_type

            if self.json_keyfile_dict is None:
                self.creds = ServiceAccountCredentials.from_json_keyfile_name(
                    self.confs_path, scopes=SCOPES)
            else:
                self.creds = ServiceAccountCredentials.from_json_keyfile_dict(
                    self.json_keyfile_dict, scopes=SCOPES)


        else:
            creds = None
            # The file token.pickle stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    self.creds = pickle.load(token)

            obj = lambda: None
            lmao = {"auth_host_name": 'localhost', 'noauth_local_webserver': 'store_true',
                    'auth_host_port': [8081, 8090],
                    'logging_level': 'ERROR'}
            for k, v in lmao.items():
                setattr(obj, k, v)

            # authorization boilerplate code
            store = file.Storage('token.json')
            self.creds = store.get()
            if not self.creds or self.creds.invalid:
                if self.auth_type == 'flow':
                    flow = client.flow_from_clientsecrets(self.confs_path, SCOPES)
                    self.creds = tools.run_flow(flow, store, obj)
        self.service = build('drive', 'v3', credentials=self.creds)

    def download_file(self, file_id, path, supports_team_drives=None, supports_all_drives=None, num_retries=50):
        """
        :param file_id: Drive file id
        :param path: path to save files
        :param supports_team_drives: whether to support or not team drives
        :param supports_all_drives: whether to support or all drives
        :param num_retries: number of retries if download leads to errors
        :return: Drive file id
        """

        request = self.service.files().get_media(fileId=file_id, supportsTeamDrives=supports_team_drives,
                                                 supportsAllDrives=supports_all_drives)

        fh = io.FileIO(path, mode='w')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk(num_retries=num_retries)
            self.logger.info("Download %d%%." % int(status.progress() * 100))
        return done

    def upload_file(self, file_path, parent_id='', mime_type='application/pdf', description=None,
                    supports_team_drives=None):
        """
        :param file_path: path for file to be uploaded
        :param parent_id: parent id for Drive folder.
        If not specified as part of a create request, the file will be placed directly in the user's My Drive folder
        :param mime_type: file mime type. Ex. application/pdf
        :param description: optional file description
        :param supports_team_drives: whether to support or not team drives
        :return: Drive file id
        """
        file_name = file_path.rsplit('/')[-1] if len(file_path.rsplit('/')) else file_path.rsplit('\\')[-1]
        file_metadata = {'name': file_name, 'parents': [parent_id], 'mimeType': mime_type, 'title': file_name}
        if description is not None:
            file_metadata['description'] = description
        media_body = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        file = self.service.files().create(
            body=file_metadata,
            media_body=media_body, fields='id', supportsTeamDrives=supports_team_drives).execute()

        self.logger.info('File ID: {}'.format(file.get('id')))
        return file.get('id')

    def rename_file(self, file_id, file_metadata, supports_team_drives=None):
        """
        :param file_id: Drive file id to rename
        :param file_metadata: the request body as specified here
        https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/drive_v3.files.html#update
        :param supports_team_drives: whether to support or not team drives
        :return: Drive file id
        """

        file = self.service.files().update(fileId=file_id,
                                           body=file_metadata, fields='id',
                                           supportsTeamDrives=supports_team_drives).execute()

        self.logger.info('File ID: {}'.format(file.get('id')))
        return file.get('id')

    def query_files(self, query="name='test.pdf' and '1Nx2yo7qWFILjDQl6aMpIyY_ej8zuQVO3' in parents",
                    fields='files(id, name)',
                    supports_team_drives=None, include_team_drive_items=None):
        """
        :param query: Drive query, the same when using Drive from browser
        :param fields: Drive fields to be returned.
        Ex. files(id, name) to get back Drive file id and file name from search
        :param supports_team_drives: whether to support or not team drives
        :param include_team_drive_items: whether to support or not return of team drives items
        :return: query results as a json response of list of files (starting with key "files": [{file1}, {file2}...]
        """
        results = self.service.files().list(q=query,
                                            spaces='drive', supportsTeamDrives=supports_team_drives,
                                            includeTeamDriveItems=include_team_drive_items,
                                            fields=fields).execute()
        return results
