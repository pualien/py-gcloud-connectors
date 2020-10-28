![](https://i.imgur.com/vCJ3s3E.png)

# PY GCLOUD CONNECTORS
Python utilities to simplify connection with Google APIs

## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/pualien/py-gcloud-connectors

Binary installers for the latest released version are available at the [Python
package index](https://pypi.org/project/gcloud-connectors/).

```sh
pip install gcloud-connectors
```

## Google Wrappers
- `BigQueryConnector` to read and cast pandas DataFrame from BigQuery

- `GAnalyticsConnector` to unsample data and return pandas DataFrame from Google Analytics

- `GDriveConnector` to download, upload, search and rename files from Google Drive

- `GSCConnector` to get data from Google Search Console

- `GSheetsConnector` to read and upload pandas DataFrame from / to Google Spreadsheet

- `GStorageConnector` to write pandas DataFrame in parquet format to Google Cloud Storage, recursive delete, copy files and folders between buckets


### Bonus

- `ForeignExchangeRatesConverter` to get currency conversion rates

- `LTVCalculator` to compute Customer Lifetime Value

- `pd_utils` to derive month, quarter column from date in pandas DataFrame