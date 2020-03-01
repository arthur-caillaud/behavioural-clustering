# External packages
import pandas as pd
import numpy as np
import datetime


def build_empty_matrix(df):
    min_date = df["Local Timestamp"].min()
    max_date = df["Local Timestamp"].max()
    date_rng = pd.date_range(min_date.date(), max_date.date(), freq="H")
    horodateur = df["Respondent ID"].unique()
    timeseries = pd.DataFrame(0, index=date_rng, columns=horodateur)
    return timeseries


def build_ts(data, ts):
    r_id = data.name
    gb = data[["Local Timestamp", "Platform"]].groupby("Local Timestamp")[
        "Platform"].count()
    ts.loc[ts.index.isin(gb.index), r_id] += gb


def outliers_cap(ts):
    temp = ts[ts != 0]
    Q1 = temp.quantile(0.15)
    Q3 = temp.quantile(0.85)
    IQR = Q3 - Q1
    outlier_limit = Q3 + 1.5 * IQR

    def helper(serie):
        r_id = serie.name
        limit = outlier_limit[r_id]
        temp = serie.copy()
        temp[temp >= limit] = limit
        return temp

    return ts.apply(helper)


def normalize_ts(ts):
    temp = ts.max()
    return ts / temp
