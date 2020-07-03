import pandas as pd
import numpy as np


class LTVCalculator:
    def __init__(self, base_df, date_order_column, date_register_column, cumsum_base_column, ltv_index_columns,
                 ltv_type='Q'):
        self.base_df = base_df.copy(deep=True)
        self.date_order_column = date_order_column
        self.date_register_column = date_register_column
        self.cumsum_base_column = cumsum_base_column
        self.base_df = self.base_df.assign(
            date_order=np.where(self.base_df[self.date_order_column].isnull(), self.base_df[self.date_register_column],
                                self.base_df[self.date_order_column])
        ).fillna(0.0)

        self.list_date_order = sorted(list(self.base_df[self.date_order_column].unique()))
        self.ltv_index_columns = ltv_index_columns
        self.ltv_type = ltv_type
        self.index_column = '{} index'.format(self.ltv_type)

        self.ltv_df = None

        pass

    def get_x_index(self, row):
        return "{}{}".format(self.ltv_type,
                             str(self.list_date_order.index(row[self.date_order_column]) - self.list_date_order.index(
                                 row[self.date_register_column]) + 1).zfill(
                                 2))

    def execute(self):
        self.base_df[self.index_column] = self.base_df.apply(lambda row: self.get_x_index(row), axis=1)
        self.base_df = self.base_df.sort_values(by=[self.index_column, self.date_register_column])
        self.base_df['cumsum'] = self.base_df.groupby(self.ltv_index_columns)[self.cumsum_base_column].transform(
            pd.Series.cumsum).round(2)

        self.ltv_df = self.base_df.pivot_table(index=self.index_column, columns=self.ltv_index_columns,
                                               values=self.cumsum_base_column,
                                               aggfunc=np.sum).cumsum().round(2)

        return self.base_df

    def pd_to_csv(self, filename, decimal=','):
        if self.base_df is None:
            raise AttributeError('LTV DataFrame not defined, please launch execute before')
        self.base_df.fillna('').to_csv(filename, index=False, decimal=decimal)
