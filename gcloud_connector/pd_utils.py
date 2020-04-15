import pandas as pd


def add_date_q_column(df, date_column='date'):
    df[date_column] = pd.to_datetime(df[date_column])
    df["date Q"] = "'" + df[date_column].dt.quarter.astype(str).str.zfill(2) + df[date_column].dt.year.astype(str).str.zfill(4)
    return df


def add_date_month_column(df, date_column='date'):
    df[date_column] = pd.to_datetime(df[date_column])
    df["date month"] = "'" + df[date_column].dt.strftime('%Y%m01')
    return df
