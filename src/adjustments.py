from google.cloud import bigquery

from fintrans_toolbox.src import bq_utils as bq
from fintrans_toolbox.src import table_utils as t


def get_adj_rphst(time_period):
    if time_period in ["Quarter", "quarter", "q"]:
        time_period = "Quarter"
    if time_period in ["Month", "month", "m"]:
        time_period = "M"
    print("Getting adjustment table")
    client = bigquery.Client()
    df_adj = t.read_retail_performance_high_streets_towns(
        client,
        time_period,
        cardholder_location_level="All",
        merchant_location_level="All",
        mcg="All",
        cardholder_location="",
        merchant_location="",
    )

    df_adj = t.create_date_time(df_adj)
    df_adj = df_adj.sort_values(by=["time_period", "date_time"])

    df_adj["index"] = df_adj.groupby(["time_period"])["cardholders"].transform(
        lambda x: x / x.iloc[0]
    )
    return df_adj


def get_adj_spoc(time_period="", cardholder_origin="", merchant_channel=""):
    if time_period in ["Quarter", "quarter", "q"]:
        time_period = "Quarter"
    if time_period in ["Month", "month", "m"]:
        time_period = "Month"
    print("Getting adjustment table")
    sql = """SELECT * FROM
        `ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel`
        WHERE (cardholder_origin = 'UNITED KINGDOM' and
        cardholder_location = 'All' and mcg = 'All')
        or (cardholder_origin = 'International Cardholder'  and mcg = 'All')
        or (cardholder_origin = 'All'  and mcg = 'All')"""
    client = bigquery.Client()
    df_adj = bq.read_bq_table_sql(client, sql)

    if time_period != "":
        df_adj = df_adj.loc[(df_adj.time_period.str.upper() == time_period.upper())]
    elif cardholder_origin != "":
        df_adj = df_adj.loc[
            (df_adj.cardholder_origin.str.upper() == cardholder_origin.upper())
        ]
    elif merchant_channel != "":
        df_adj = df_adj.loc[
            (df_adj.merchant_channel.str.upper() == merchant_channel.upper())
        ]
    else:
        pass

    df_adj = (
        df_adj.groupby(
            [
                "time_period",
                "time_period_value",
                "cardholder_origin",
                "cardholder_origin_country",
                "mcg",
                "mcc",
                "merchant_channel",
            ]
        )
        .sum(["spend", "transactions", "cardholders"])
        .reset_index()
    )

    df_adj = t.create_date_time(df_adj)
    df_adj = df_adj.sort_values(
        by=[
            "cardholder_origin",
            "cardholder_origin_country",
            "merchant_channel",
            "time_period",
            "date_time",
        ]
    )

    df_adj["index"] = df_adj.groupby(
        [
            "cardholder_origin",
            "cardholder_origin_country",
            "merchant_channel",
            "time_period",
        ]
    )["cardholders"].transform(lambda x: x / x.iloc[0])

    df_adj.loc[
        df_adj["cardholder_origin"] == "UNITED KINGDOM", "merged_country"
    ] = "UNITED KINGDOM"
    df_adj.loc[
        df_adj["cardholder_origin"] != "UNITED KINGDOM", "merged_country"
    ] = df_adj.cardholder_origin_country
    return df_adj


def get_adj_sml(
    time_period="", cardholder_issuing_level="", cardholder_issuing_country=""
):

    if time_period in ["Quarter", "quarter", "q"]:
        time_period = "Quarter"
    if time_period in ["Month", "month", "m"]:
        time_period = "Month"
    print("Getting adjustment table")
    sql = """SELECT *
        FROM `ons-fintrans-data-prod.fintrans_visa.spend_merchant_location`
        WHERE merchant_location_level = 'All'  and mcg = 'All'"""
    client = bigquery.Client()
    df_adj = bq.read_bq_table_sql(client, sql)

    if time_period != "":
        df_adj = df_adj.loc[(df_adj.time_period.str.upper() == time_period.upper())]
    elif cardholder_issuing_level != "":
        df_adj = df_adj.loc[
            (
                df_adj.cardholder_issuing_level.str.upper()
                == cardholder_issuing_level.upper()
            )
        ]
    else:
        pass

    df_adj = (
        df_adj.groupby(
            [
                "time_period",
                "time_period_value",
                "cardholder_issuing_level",
                "cardholder_issuing_country",
            ]
        )
        .sum(["spend", "transactions", "cardholders"])
        .reset_index()
    )

    df_adj = t.create_date_time(df_adj)
    df_adj = df_adj.sort_values(
        by=[
            "cardholder_issuing_level",
            "cardholder_issuing_country",
            "time_period",
            "date_time",
        ]
    )

    df_adj["index"] = df_adj.groupby(
        ["cardholder_issuing_level", "cardholder_issuing_country", "time_period"]
    )["cardholders"].transform(lambda x: x / x.iloc[0])
    return df_adj


def try_to_merge(merge_on, df, df_adj):

    try:
        df1 = df.merge(
            df_adj[["date_time"] + merge_on + ["index"]],
            on=["date_time"] + merge_on,
            how="left",
        )
        print(f"Merging on date_time, {merge_on}")
    except Exception as e:
        print(f"{e}: using time_period_value, consider converting to date_time")
        print(f"Merging on time_period_value, {merge_on}")
        df1 = df.merge(
            df_adj[["time_period_value"] + merge_on + ["index"]],
            on=["time_period_value"] + merge_on,
            how="left",
        )

    try:
        df1["idx_spend"] = df1["spend"] / df1["index"]
    except Exception as e:
        print(f"{e}")
    try:
        df1["idx_transactions"] = df1["transactions"] / df1["index"]
    except Exception as e:
        print(f"{e}")

    return df1


def adjusted_rphst(df, time_period):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe of interest that you have read in
       - time_period: "Month" or "Quarter" or "" for both
    Returns:
       - the dataframe with adjusted spend/transactions if that variable exists
    """
    print("RPHST adjusted by number of cardholders in time period")
    df_adj = get_adj_rphst(time_period)
    merge_on = ["time_period"]
    # link on datetime first
    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("output table is different legth to input table, check merge")

    return df1


def adjusted_spoc(df, time_period="", cardholder_origin="", merchant_channel=""):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe to be adjusted
       - time_period: "Month" or "Quarter" or "" for both
       - cardholder_origin: "All", "International Cardholder" or "United Kingdom".
       Defaults to "".
       - Merchant channel: "All", "Online" or "Face to Face"
    Returns:
       - the dataframe with adjusted spend/transactions if that variable exists
    """
    print("Spend origin and channel values are adjusted to Jan 2019 cardholders.")
    print("n of cardholders with the same origin country and merchant channel")
    df_adj = get_adj_spoc(time_period)

    # If cardholder origin "internation" and
    # cardholder issueing country doesn't exist, fail
    if cardholder_origin == "International Cardholder" and not any(
        df.columns == "cardholder_origin_country"
    ):
        print("cardholder_origin_country col does not exist.")
        print("Need to match on country for international")
    else:
        pass

    if set(["cardholder_origin_country", "cardholder_origin"]).issubset(df.columns):
        df.loc[
            df["cardholder_origin"] == "UNITED KINGDOM", "merged_country"
        ] = "UNITED KINGDOM"
        df.loc[
            df["cardholder_origin"] != "UNITED KINGDOM", "merged_country"
        ] = df.cardholder_origin_country
        df = df.drop(columns=["cardholder_origin_country", "cardholder_origin"])
    elif cardholder_origin == "":
        print(
            "cardholder_origin_country and cardholder_origin are not on the dataframe"
        )
    else:
        df = df.assign(merged_country=cardholder_origin)

    if merchant_channel == "" and not any(df_adj.columns == "merchant_channel"):
        print("there is no column merchant_channel in df")

    else:
        cols = ["time_period", "merged_country", "merchant_channel"]
        merge_on = list(set(cols) & set(df_adj.columns))

    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("output table is different legth to input table, check merge")

    return df1


def adjusted_sml(
    df, time_period="", cardholder_issuing_level="", cardholder_issuing_country=""
):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe to be adjusted
       - time_period:  "Month" or "Quarter" or "" for both
       - cardholder_issuing_level: "All", "International" or "Domestic".
       Defaults to any, where all of these will be included.
       - cardholder_issuing_country
    Returns:
       - the dataframe with adjusted spend/transactions
       if that variable exists
    """
    print("Spend merchant location values are adjusted to 01-2019 cardholder numbers.")
    print("Based in numbers of cardholder with the same cardholder origin country")

    df_adj = get_adj_sml(time_period)

    # TODO: If you specify cardholder_issuing_level you may
    # wish to drop the column as this is implied.
    # However it is then dificult to make the join work.
    if not (
        set(["cardholder_issuing_country", "cardholder_issuing_level"]).issubset(
            df.columns
        )
    ):
        print("cardholder_issuing_country and cardholder_issuing_level are needed")

    else:
        pass

    cols = ["time_period", "cardholder_issuing_country", "cardholder_issuing_level"]
    merge_on = list(set(cols) & set(df_adj.columns))
    # link on datetime, cardholder origin, issuing country and merchant channel first

    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("FAILED: output table is different length to input table, check merge. ")
    else:
        return df1


# Adjusts by merchant channel = all for all df
def get_adj_all(time_period="", cardholder_origin="", merchant_channel=""):
    if time_period in ["Quarter", "quarter", "q"]:
        time_period = "Quarter"
    if time_period in ["Month", "month", "m"]:
        time_period = "Month"
    print("Getting adjustment table")
    sql = """SELECT * FROM
        `ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel`
        WHERE (cardholder_origin = 'UNITED KINGDOM' and
        merchant_channel = 'All' and
        destination_country = 'UNITED KINGDOM' and
        cardholder_location = 'All' and mcg = 'All')
        or (cardholder_origin = 'International Cardholder' and
        mcg = 'All' and merchant_channel = 'All')
        or (cardholder_origin = 'All'  and mcg = 'All' and merchant_channel = 'All')
        """
    client = bigquery.Client()
    df_adj = bq.read_bq_table_sql(client, sql)

    if time_period != "":
        df_adj = df_adj.loc[(df_adj.time_period.str.upper() == time_period.upper())]
    elif cardholder_origin != "":
        df_adj = df_adj.loc[
            (df_adj.cardholder_origin.str.upper() == cardholder_origin.upper())
        ]
    elif merchant_channel != "":
        df_adj = df_adj.loc[
            (df_adj.merchant_channel.str.upper() == merchant_channel.upper())
        ]
    else:
        pass

    df_adj = (
        df_adj.groupby(
            [
                "time_period",
                "time_period_value",
                "cardholder_origin",
                "cardholder_origin_country",
                "mcg",
                "mcc",
                "merchant_channel",
            ]
        )
        .sum(["spend", "transactions", "cardholders"])
        .reset_index()
    )

    df_adj = t.create_date_time(df_adj)
    df_adj = df_adj.sort_values(
        by=[
            "cardholder_origin",
            "cardholder_origin_country",
            "merchant_channel",
            "time_period",
            "date_time",
        ]
    )

    df_adj["index"] = df_adj.groupby(
        [
            "cardholder_origin",
            "cardholder_origin_country",
            #            "merchant_channel",
            "time_period",
        ]
    )["cardholders"].transform(lambda x: x / x.iloc[0])

    df_adj.loc[
        df_adj["cardholder_origin"] == "UNITED KINGDOM", "merged_country"
    ] = "UNITED KINGDOM"
    df_adj.loc[
        df_adj["cardholder_origin"] != "UNITED KINGDOM", "merged_country"
    ] = df_adj.cardholder_origin_country
    return df_adj


def adjusted_all_rphst(df, time_period):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe of interest that you have read in
       - time_period: "Month" or "Quarter" or "" for both
    Returns:
       - the dataframe with adjusted spend/transactions if that variable exists
    """
    print("RPHST adjusted by number of cardholders in time period")

    df["merged_country"] = "UNITED KINGDOM"

    df_adj = get_adj_all(time_period)
    merge_on = ["time_period", "merged_country"]
    # link on datetime first
    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("output table is different legth to input table, check merge")

    return df1


def adjusted_all_spoc(df, time_period="", cardholder_origin="", merchant_channel=""):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe to be adjusted
       - time_period: "Month" or "Quarter" or "" for both
       - cardholder_origin: "All", "International Cardholder" or "United Kingdom".
       Defaults to "".
       - Merchant channel: "All", "Online" or "Face to Face"
    Returns:
       - the dataframe with adjusted spend/transactions if that variable exists
    """
    print("Spend origin and channel values are adjusted to Jan 2019 cardholders.")
    print("n of cardholders with the same origin country and merchant channel")
    df_adj = get_adj_all(time_period)

    # If cardholder origin "internation" and
    # cardholder issueing country doesn't exist, fail
    if cardholder_origin == "International Cardholder" and not any(
        df.columns == "cardholder_origin_country"
    ):
        print("cardholder_origin_country col does not exist.")
        print("Need to match on country for international")
    else:
        pass

    if set(["cardholder_origin_country", "cardholder_origin"]).issubset(df.columns):
        df.loc[
            df["cardholder_origin"] == "UNITED KINGDOM", "merged_country"
        ] = "UNITED KINGDOM"
        df.loc[
            df["cardholder_origin"] != "UNITED KINGDOM", "merged_country"
        ] = df.cardholder_origin_country
        df = df.drop(columns=["cardholder_origin_country", "cardholder_origin"])
    elif cardholder_origin == "":
        print(
            "cardholder_origin_country and cardholder_origin are not on the dataframe"
        )
    else:
        df = df.assign(merged_country=cardholder_origin)

    if merchant_channel == "" and not any(df_adj.columns == "merchant_channel"):
        print("there is no column merchant_channel in df")

    else:
        cols = ["time_period", "merged_country"]
        merge_on = list(set(cols) & set(df_adj.columns))

    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("output table is different legth to input table, check merge")

    return df1


def adjusted_all_sml(
    df, time_period="", cardholder_issuing_level="", cardholder_issuing_country=""
):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       - df: the dataframe to be adjusted
       - time_period:  "Month" or "Quarter" or "" for both
       - cardholder_issuing_level: "All", "International" or "Domestic".
       Defaults to any, where all of these will be included.
       - cardholder_issuing_country
    Returns:
       - the dataframe with adjusted spend/transactions
       if that variable exists
    """
    print("Spend merchant location values are adjusted to 01-2019 cardholder numbers.")
    print("Based in numbers of cardholder with the same cardholder origin country")

    df["merged_country"] = df["cardholder_issuing_country"]
    df_adj = get_adj_all(time_period)

    # TODO: If you specify cardholder_issuing_level you may
    # wish to drop the column as this is implied.
    # However it is then dificult to make the join work.
    if not (
        set(["cardholder_issuing_country", "cardholder_issuing_level"]).issubset(
            df.columns
        )
    ):
        print("cardholder_issuing_country and cardholder_issuing_level are needed")

    else:
        pass

    cols = ["time_period", "merged_country"]
    merge_on = list(set(cols) & set(df_adj.columns))
    # link on datetime, cardholder origin, issuing country and merchant channel first

    df1 = try_to_merge(merge_on, df, df_adj)

    if len(df1) != len(df):
        print("FAILED: output table is different length to input table, check merge. ")
    else:
        return df1
