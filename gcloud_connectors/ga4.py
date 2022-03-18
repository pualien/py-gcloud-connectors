import os
# Set environment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "service_account_file_path"

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
import pandas as pd

import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from retry import retry

from gcloud_connectors.logger import EmptyLogger

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class GAError(Exception):
    """Base class for GA exceptions"""


class GAPermissionError(GAError):
    """Base class for GA permission exceptions"""
    pass

def sample_run_report(property_id, export_path):
    """Runs a simple report on a Google Analytics 4 property."""

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2020-03-31", end_date="today")],
    )
    response = client.run_report(request)

    output = []
    print("Report result:")
    for row in response.rows:
        output.append({"City": row.dimension_values[0].value, "Active Users": row.metric_values[0].value})
    df = pd.DataFrame(output)
    df.to_csv(export_path)


sample_run_report(PROPERTY_ID, "export.csv")
df = pd.read_csv("export.csv")
df.head(5)