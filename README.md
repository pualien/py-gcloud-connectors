![](https://i.imgur.com/vCJ3s3E.png)

# PY GCLOUD CONNECTORS
[![PyPI Latest Release](https://img.shields.io/pypi/v/gcloud-connectors.svg)](https://pypi.org/project/gcloud-connectors/)
[![PyPI Build](https://github.com/pualien/py-gcloud-connectors/workflows/PyPI%20Build/badge.svg)](https://github.com/pualien/py-gcloud-connectors/actions)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/gcloud-connectors)](https://pypi.org/project/gcloud-connectors/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/gcloud-connectors.svg)](https://pypi.org/project/gcloud-connectors/)

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
- `BigQueryConnector`: read and cast pandas DataFrame from BigQuery

- `GAnalyticsConnector`: unsample data and return pandas DataFrame from Google Analytics

- `GDriveConnector`: download, upload, search and rename files from Google Drive

- `GSCConnector`: get data from Google Search Console

- `GSheetsConnector`: read and upload pandas DataFrame from / to Google Spreadsheet

- `GStorageConnector`: write pandas DataFrame in parquet format to Google Cloud Storage, recursive delete, copy files and folders between buckets


### Bonus

- `ForeignExchangeRatesConverter`: get currency conversion rates

- `LTVCalculator`: compute Customer Lifetime Value

- `pd_utils`: derive quarter, month column from date in pandas DataFrame