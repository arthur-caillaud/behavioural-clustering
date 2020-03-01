# External packages
import pandas as pd
import numpy as np
import datetime


def concatenating_all_platform():
    # Get and format Mobile entertainment
    df_mobile_cons = pd.read_csv(
        "data/mobile_entertainment.txt", sep="\t", header=0, index_col=False)
    mobile_cons_cols = ["Respondent ID", "Device ID", "Local Timestamp",
                        "Video Length", "Time Of Day", "Service", "Action"]
    df_mobile_cons = df_mobile_cons[mobile_cons_cols]
    df_mobile_cons.Service = df_mobile_cons.Service.str.lower()
    df_mobile_cons["Platform"] = "Mobile"

    # Get and format pc entertainment
    df_pc_cons = pd.read_csv("data/pc_traffic.txt",
                             sep="\t", header=0, index_col=False)
    pc_cons_cols = ["Respondent ID", "Device ID", "Local Timestamp",
                    "Duration", "Time Of Day", "Domain", "Action"]
    df_pc_cons = df_pc_cons[pc_cons_cols]
    df_pc_cons.columns = mobile_cons_cols
    df_pc_cons.Service = df_pc_cons.Service.str.lower()
    df_pc_cons["Platform"] = "Web"

    df_pc_cons["Service"] = df_pc_cons["Service"].str[:-4]
    df_pc_cons.loc[(df_pc_cons["Service"] == "www.youtube"),
                   "Service"] = "youtube"
    df_pc_cons["Service"].unique()
    total_cons = pd.concat((df_mobile_cons, df_pc_cons))

    # Filter Play
    total_cons = total_cons[total_cons.Action == "Play"]
    total_cons["Local Timestamp"] = pd.to_datetime(
        total_cons["Local Timestamp"], format="%Y-%m-%d %H:%M:%S")
    return total_cons


def creating_dataset(df_all):
    # Calculate Floor
    df_all["Local Timestamp floored"] = pd.to_datetime(
        df_all["Local Timestamp"]).dt.floor("D")
    gb = df_all.groupby("Respondent ID")

    def compute_delta(df):
        temp = df.copy()
        temp.sort_values(by="Local Timestamp", inplace=True)
        temp["delta"] = df["Local Timestamp floored"].diff()
        temp["delta"] = temp["delta"].shift(-1)
        temp = temp[(temp["delta"].dt.days <= 7) & (temp["delta"].dt.days > 0)]
        temp = temp[temp["Local Timestamp"] == (temp["Local Timestamp"].max())]
        return temp[["Local Timestamp", "delta"]]

    # Get Max dates for each Respondent
    r_dates_delta = gb.apply(compute_delta)
    r_dates_delta.index = r_dates_delta.index.droplevel(1)
    r_dates_delta = r_dates_delta[~pd.isnull(r_dates_delta['delta'])]
    print(r_dates_delta["delta"].value_counts())
    n_1_dates = r_dates_delta["Local Timestamp"]

    def filter_min(df):
        r_id = df["Respondent ID"].any()
        return df[df['Local Timestamp'] <= n_1_dates.loc[r_id]]

    def filter_max(df):
        r_id = df["Respondent ID"].any()
        return df[df['Local Timestamp'] > n_1_dates.loc[r_id]]

    # index R_id, value cut moment
    gb = df_all[df_all["Respondent ID"].isin(
        r_dates_delta.index)].groupby("Respondent ID")
    temp_X = gb.apply(filter_min)
    temp_X.index = temp_X.index.droplevel(0)
    temp_Y = gb.apply(filter_max)
    temp_Y.index = temp_Y.index.droplevel(0)

    def helper_2(df):
        temp = df.copy()
        row = temp[temp["Local Timestamp"] == temp["Local Timestamp"].min()]
        r_id = df["Respondent ID"].any()
        return pd.Series([row["Respondent ID"].any(), row["Platform"].any(), row["Time Of Day"].any(), r_dates_delta.loc[r_id, "delta"]], index=["Respondent ID", "Platform", "Time Of Day", "Days"])

    y = temp_Y.groupby("Respondent ID").apply(helper_2)
    y = y.reset_index(drop=True)
    y["Days"] = y["Days"].dt.days
    return temp_X, y
