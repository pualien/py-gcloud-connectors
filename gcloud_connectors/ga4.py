import google.auth
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2 import service_account

from gcloud_connectors.logger import EmptyLogger

SCOPES = ['https://www.googleapis.com/auth/analytics']


class GAnalytics4Connector:
    def __init__(self, confs_path=None, auth_type='service_account', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type
        self.logger = logger if logger is not None else EmptyLogger()

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
            self.creds, project = google.auth.default()

        self.logger = logger if logger is not None else EmptyLogger()
        self.service = BetaAnalyticsDataClient(credentials=self.creds)

    @staticmethod
    def get_base_report_params(property_id, metrics_dict_values, dimensions_dict_values, date_range, limit, offset,
                               keep_empty_rows, return_property_quota):
        return {
            "property": "properties/{property_id}".format(property_id=property_id),
            "dimensions": dimensions_dict_values,
            "metrics": metrics_dict_values,
            "date_ranges": date_range,
            "limit": limit,
            "offset": offset,
            "keep_empty_rows": keep_empty_rows,
            "return_property_quota": return_property_quota
        }

    @staticmethod
    def get_filter_type(filter_type_string):
        filter_type = Filter.StringFilter  # default
        if filter_type_string == "string_filter":
            filter_type = Filter.StringFilter
        elif filter_type_string == "in_list_filter":
            filter_type = Filter.InListFilter
        elif filter_type_string == "numeric_filter":
            filter_type = Filter.NumericFilter
        elif filter_type_string == "between_filter":
            filter_type = Filter.BetweenFilter
        return filter_type

    def get_filters(self, filters):
        """
        filters = [{"field_name": "browser", "type": "string_filter", "value": "chrome", "match_type": 1,
        "is_not": True}]
        """
        filter_dict_values = []
        for filter_object in filters:
            filter_type = self.get_filter_type(filter_object["type"])
            if filter_object.get("match_type"):
                filter_applied = filter_type(
                    {"value": filter_object["value"], "match_type": filter_object["match_type"]}
                )
            else:
                if filter_object["type"] == 'in_list_filter':
                    filter_applied = filter_type({"values": filter_object["values"]})
                else:
                    filter_applied = filter_type({"value": filter_object["value"]})

            # TODO: wait to be fixed combining not_expressions inside FilterExpressionList. In march 2022 leads to
            #  TypeError: Parameter to MergeFrom() must be instance of same class: expected FilterExpression
            #  got Filter. for field FilterExpression.not_expression
            if filter_object.get("is_not"):
                filter_dict_values.append(
                    FilterExpression({
                        "not_expression": Filter({
                            "field_name": filter_object["field_name"],
                            filter_object["type"]: filter_applied

                        }
                        )
                    })
                )
            else:
                filter_dict_values.append(
                    FilterExpression({
                        "filter": Filter({
                            "field_name": filter_object["field_name"],
                            filter_object["type"]: filter_applied

                        }
                        )
                    })
                )
        return filter_dict_values

    @staticmethod
    def get_df_from_response(response, dimensions, metrics):
        output = []
        for row in response.rows:
            current_row = {}
            for dim in dimensions:
                current_row[dim] = row.dimension_values[dimensions.index(dim)].value
            for metr in metrics:
                current_row[metr] = row.metric_values[metrics.index(metr)].value
            output.append(current_row)
        return pd.DataFrame(output)

    def pd_get_report(self, property_id, start_date, end_date, metrics, dimensions, dimension_filters=None,
                      metrics_filters=None, offset=0, limit=100000,
                      keep_empty_rows=False, return_property_quota=False, downloaded_totals=0, cast_date_column=None):
        """Runs a complex report on a Google Analytics 4 property."""
        dimensions_dict_values = [Dimension({"name": x}) for x in dimensions]
        metrics_dict_values = [Metric({"name": x}) for x in metrics]
        date_range = [DateRange({"start_date": start_date, "end_date": end_date})]

        if dimension_filters is None:
            dimension_filters = []
        if metrics_filters is None:
            metrics_filters = []

        dimension_filter_dict_values = self.get_filters(dimension_filters)
        metric_filter_dict_values = self.get_filters(metrics_filters)

        report_params = self.get_base_report_params(property_id=property_id, metrics_dict_values=metrics_dict_values,
                                                    dimensions_dict_values=dimensions_dict_values,
                                                    date_range=date_range, limit=limit, offset=offset,
                                                    keep_empty_rows=keep_empty_rows,
                                                    return_property_quota=return_property_quota)
        if len(dimension_filter_dict_values):
            report_params["dimension_filter"] = FilterExpression({
                "and_group": FilterExpressionList(
                    {
                        "expressions": dimension_filter_dict_values

                    }
                )
            })
        if len(metric_filter_dict_values):
            report_params["metric_filter"] = FilterExpression({
                "and_group": FilterExpressionList(
                    {
                        "expressions": metric_filter_dict_values
                    }
                )
            })

        request = RunReportRequest(report_params)

        response = self.service.run_report(request)

        df = self.get_df_from_response(response, dimensions, metrics)

        # if missing pages for all rows in response.row_count
        if len(response.rows) < response.row_count and downloaded_totals < response.row_count:
            downloaded_totals = len(response.rows) + downloaded_totals
            print(
                "Recursive iteration page {current_page}/{pages} for records {records}/{totals}".format(
                    records=offset + limit if offset + limit < response.row_count else response.row_count,
                    totals=response.row_count,
                    current_page=round(downloaded_totals / limit),
                    pages=round(response.row_count / limit)
                ))
            df = df.append(
                self.pd_get_report(property_id=property_id, start_date=start_date, end_date=end_date, metrics=metrics,
                                   dimensions=dimensions, dimension_filters=dimension_filters,
                                   metrics_filters=metrics_filters, offset=offset + limit, limit=limit,
                                   keep_empty_rows=keep_empty_rows, return_property_quota=return_property_quota,
                                   downloaded_totals=downloaded_totals))
        if cast_date_column:
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], format="%Y%m%d").dt.date
        return df
