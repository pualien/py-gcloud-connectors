from __future__ import print_function

import io
import pickle
import os.path
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive']


class GDriveConnector():
    def __init__(self, confs_path, auth_type='flow'):
        self.confs_path = confs_path
        self.auth_type = auth_type

        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        obj = lambda: None
        lmao = {"auth_host_name": 'localhost', 'noauth_local_webserver': 'store_true', 'auth_host_port': [8080, 8090],
                'logging_level': 'ERROR'}
        for k, v in lmao.items():
            setattr(obj, k, v)

        # authorization boilerplate code
        store = file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            if self.auth_type == 'flow':
                flow = client.flow_from_clientsecrets(self.confs_path, SCOPES)
                creds = tools.run_flow(flow, store, obj)

        self.service = build('drive', 'v3', credentials=creds)

    def download_file(self, file_id, path, num_retries=50):

        request = self.service.files().get_media(fileId=file_id)

        # replace the filename and extension in the first field below
        fh = io.FileIO(path, mode='w')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk(num_retries=num_retries)
            print("Download %d%%." % int(status.progress() * 100))
        return done

