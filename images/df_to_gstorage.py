





from gcloud_connectors.gstorage import GStorageConnector



gstorage_service = GStorageConnector(confs_path=None)



... load df



gstorage_service.pd_to_gstorage(
    df=df, bucket_name=bucket_name,
    file_name_path='{table_suffix}/y={year}/m={month}/d={day}/'
                   'event_date={event_date}/'
                   'string_col={string_col}/data.parquet'.format(
    table_suffix=table_suffix, year=day.strftime('%Y'),
    month=day.strftime('%m'), day=day.strftime('%d'),
    event_date=day.strftime('%Y-%m-%d'),
    string_col=string_col))

