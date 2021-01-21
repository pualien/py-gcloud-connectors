
from google.cloud.bigquery_storage_v1beta1 import BigQueryStorageClient
from google.oauth2 import service_account
from google.cloud import bigquery


class BigQueryConnector:
    def __init__(self, project_id, confs_path=None, auth_type='service_account', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type
        self.project_id = project_id
        # authorization boilerplate code
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        if self.confs_path is not None or self.json_keyfile_dict is not None:
            if self.confs_path is not None:
                self.creds = service_account.Credentials.from_service_account_file(
                    self.confs_path,
                )
            elif self.json_keyfile_dict is not None:
                self.creds = service_account.Credentials.from_service_account_info(
                    self.json_keyfile_dict
                )

        else:
            self.creds = None
        self.service = bigquery.Client(project=self.project_id, credentials=self.creds)

    @staticmethod
    def pd_cast_dtypes(df, table_dtypes):
        """
        :param df: pandas DataFrame coming from BigQuery sql
        :param table_dtypes: pandas dtypes
        :return: pandas DataFrame casted
        """
        table_dtypes = {col: d_type for col, d_type in table_dtypes.items() if col in df.columns}
        try:
            df = df.astype(table_dtypes)
        except Exception as e:
            print(e)
            for col, d_type in table_dtypes.items():
                if col in df.columns:
                    try:
                        if d_type == 'integer':
                            d_type = int
                        if d_type == 'float':
                            d_type = float
                        if d_type == 'string' or d_type == 'category' or d_type == str:
                            d_type = str
                        if d_type in (str, float, int):
                            if d_type in (float, int):
                                pass
                                # df[col] = pd.to_numeric(df[col], errors='coerce')
                            df[col] = df[col].astype(d_type)
                        if d_type in ('boolean', 'bool'):
                            try:
                                df[col] = df[col].astype(int).fillna(False)
                            except Exception as e:
                                pass
                            df[col] = df[col].replace({1: True, 0: False})
                    except Exception as e:
                        print('No casting for {} into {} from {}'.format(col, d_type, df[col].dtype))
                        print(e)
        return df

    def pd_get_dtypes(self, dataset, table):
        """
        :param dataset: BigQuery dataset name
        :param table: BigQuery table name
        :return: dict of {col1: dtype1, col2: dtype1, col3: dtype3....}
        """
        table_ref = '{}.{}'.format(dataset, table)
        table = self.service.get_table(table_ref)
        schema_dtypes = {}
        for schema in table.schema:
            if schema.field_type in ['STRING']:
                type = str
            elif schema.field_type in ['DATE', 'TIMESTAMP']:
                type = 'datetime64[ns]'
            elif schema.field_type in ['FLOAT']:
                type = schema.field_type.lower()
            elif schema.field_type in ['INTEGER']:
                type = 'int'
            elif schema.field_type in ['BOOLEAN']:
                type = 'bool'
            else:
                raise AttributeError('Unknown type {} for {}'.format(schema.field_type, schema.name))
            schema_dtypes[schema.name] = type
        return schema_dtypes

    def get_pandas_dtypes(self, dataset, table):
        table_ref = '{}.{}'.format(dataset, table)
        table = self.service.get_table(table_ref)
        schema_dtypes = {}
        for schema in table.schema:
            if schema.field_type in ['STRING']:
                type = str
            elif schema.field_type in ['DATE', 'TIMESTAMP']:
                type = 'datetime64[ns]'
            elif schema.field_type in ['FLOAT']:
                type = schema.field_type.lower()
            elif schema.field_type in ['INTEGER']:
                type = 'int'
            elif schema.field_type in ['BOOLEAN']:
                type = 'bool'
            else:
                raise AttributeError('Unknown type {} for {}'.format(schema.field_type, schema.name))
            schema_dtypes[schema.name] = type
        return schema_dtypes

    def pd_execute(self, query, progress_bar_type=None, bqstorage_enabled=False):
        """
        :param query: sql query
        :param progress_bar_type:
        :param bqstorage_enabled: whether to user or not bqstorage to download results more quickly
        :return: pandas DataFrame from BigQuery sql execution
        """

        if bqstorage_enabled is True:
            bqstorage_client = BigQueryStorageClient(credentials=self.creds)
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


