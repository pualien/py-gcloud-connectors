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


class GDriveConnector():
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
            lmao = {"auth_host_name": 'localhost', 'noauth_local_webserver': 'store_true', 'auth_host_port': [8081, 8090],
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

    def download_file(self, file_id, path, num_retries=50):

        request = self.service.files().get_media(fileId=file_id)

        # replace the filename and extension in the first field below
        fh = io.FileIO(path, mode='w')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk(num_retries=num_retries)
            self.logger.info("Download %d%%." % int(status.progress() * 100))
        return done

    def upload_file(self, file_path, parent_id='', mime_type='application/pdf', description=None):
        file_name = file_path.rsplit('/')[-1] if len(file_path.rsplit('/')) else file_path.rsplit('\\')[-1]
        file_metadata = {'name': file_name, 'parents': [parent_id], 'mimeType': mime_type, 'title': file_name}
        # file_metadata = {'name': file_name, 'mimeType': mime_type, 'title': file_name}

        if description is not None:
            file_metadata['description'] = description
        media_body = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        # file = self.service.files().create(body=file_metadata,
        #                                     media_body=media_body,
        #                                     fields='id').execute()
        file = self.service.files().create(
            body=file_metadata,
            media_body=media_body, fields='id').execute()

        self.logger.info('File ID: {}'.format(file.get('id')))
        return file.get('id')


