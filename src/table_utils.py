from fintrans_toolbox.src import bq_utils as bq

import pandas as pd
import numpy as np


def read_spend_merchant_location(
    client,
    time_period="Quarter",
    merchant_location_level="All",
    cardholder_issuing_level="All",
    mcg="All",
    mcc="All",
    merchant_location="",
    cardholder_issuing_country="",
):
    """
    Description:
    Function allows you to read in data from the spend merchant location table\
    with default values set to 'All' where possible.
    so if you wanted to keep the defaults you would run:\
    read_spend_merchant_location(client). However, if you wanted to change
    one or more of the categorical variables you would run\
    read_spend_merchant_location(client, merchant_location_level = 'POSTAL_SECTOR').\
    If you want a value to cover anything then define it as an empty string e.g.\
    mcg = ''

    Args:
    - client: defined earlier in the session
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    - merchant_location_level: string. Defaults to 'All'
    - cardholder_issuing_level: string. Defaults to 'All'
    - mcg: string. Defaults to 'All'
    - mcc: string. Defaults to 'All'
    - merchant_location: string. defaults to '' so that all
    merchant locations are picked up
    - cardholder_issuing_country: string. defaults to '' so that all cardholder
    issuing countries are picked up.

    Returns:
       Spend Merchant Location with specification of your choice
    """

    if merchant_location != "":
        SQL2 = f" AND merchant_location IN ({merchant_location})"
        merchant_location_level = ""
    else:
        SQL2 = ""

    if cardholder_issuing_country != "":
        SQL3 = f" AND cardholder_issuing_country IN ({cardholder_issuing_country})"
        cardholder_issuing_level = ""
    else:
        SQL3 = ""

    if time_period == "Quarter":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS\
        date_time, "
    elif time_period == "Month":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS\
        date_time, "
    else:
        SQL_date = " "

    SQL1 = f"\
        SELECT {SQL_date} * , FROM\
        ons-fintrans-data-prod.fintrans_visa.spend_merchant_location\
        WHERE\
        time_period LIKE '{time_period}%' AND\
        cardholder_issuing_level LIKE '{cardholder_issuing_level}%' AND\
        merchant_location_level LIKE '{merchant_location_level}%' AND\
        mcg LIKE '{mcg}%' AND\
        mcc LIKE '{mcc}%'"

    SQL4 = " ORDER BY time_period, time_period_value"

    SQL = SQL1 + SQL2 + SQL3 + SQL4

    print(f"The SQL statement you have just selected is: {SQL}")

    df = bq.read_bq_table_sql(client, SQL)

    return df


def read_spend_origin_and_channel(
    client,
    time_period="Quarter",
    cardholder_origin="All",
    cardholder_origin_country="All",
    mcg="All",
    merchant_channel="All",
    mcc="All",
    cardholder_location="",
    destination_country="",
):
    """
    Description:
    Function allows you to read in data from the spend_origin_and_channel\
    table with default values set to 'All' where possible.
    so if you wanted to keep the defaults you would run:\
    read_spend_origin_and_channel(client). However, if you wanted to change
    one or more of the categorical variables you would run\
    read_spend_origin_and_channel(client, merchant_channel = 'Face to Face').\
    If you want a value to cover anything then define it as an empty string e.g.\
    mcg = ''

    Args:
    - client: defined earlier in the session
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    - cardholder_origin: string. Defaults to 'All'
    - cardholder_origin_country: string. Defaults to 'All'. If not 'All', it changes
    cardholder origin to be "".
    - mcg: string. Defaults to 'All'
    - mcc: boolean. Defaults to True. If False it will gather all mccs
    under the MCG specified.
    - merchant_channel: string. Defaults to 'All'
    - cardholder_location: string. defaults to '', meaning it can take any value.
    To pick a specific cardholder location(s)
    run it in the following format: cardholder_location = " 'SW2' " or
    cardholder_location = " 'SW2', 'SE4' " for multiple
    - destination_country: string. defaults to '', meaning it can take any value.
    to pick a specific destination country(s)
    run it in the following format: destination_country = " 'UNITED KINGDOM' " or
    destination_country = " 'UNITED KINGDOM', 'FRANCE' " for multiple

    Returns:
    Spend Merchant Location with specification of your choice
    """

    if cardholder_location != "":
        SQL2 = f" AND cardholder_location IN ({cardholder_location})"
        cardholder_origin = ""
        cardholder_origin_country = ""
    else:
        SQL2 = ""

    if cardholder_origin_country != "All":
        cardholder_origin = ""

    if destination_country != "":
        SQL3 = f" AND destination_country IN ({destination_country})"
    else:
        SQL3 = ""

    if time_period == "Quarter":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS\
        date_time, "
    elif time_period == "Month":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS\
        date_time, "
    else:
        SQL_date = " "

    SQL1 = f"\
        SELECT {SQL_date} * FROM\
        ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel\
        WHERE\
        time_period LIKE '{time_period}%' AND\
        cardholder_origin LIKE '{cardholder_origin}%' AND\
        cardholder_origin_country LIKE '{cardholder_origin_country}%' AND\
        mcg LIKE '{mcg}%' AND\
        mcc LIKE '{mcc}%' AND\
        merchant_channel LIKE '{merchant_channel}%'"

    SQL4 = " ORDER BY time_period, time_period_value"

    SQL = SQL1 + SQL2 + SQL3 + SQL4

    df = bq.read_bq_table_sql(client, SQL)

    print(
        "Warning: There is no default value for 'destination_country' \
    because there is no 'All' value for this categorical variable.\
    Therefore we would recommend running the following line to sum up\
    over this variable:\
    df = df.groupby\
    (['time_period', 'time_period_value', 'cardholder_origin',\
    'cardholder_origin_country', 'mcg', 'mcc', 'merchant_channe']).\
    sum(['spend, 'transactions', 'cardholders'])\
    .reset_index()\
    "
    )

    return df


def read_retail_performance_high_streets_towns(
    client,
    time_period="Quarter",
    cardholder_location_level="All",
    merchant_location_level="All",
    mcg="All",
    cardholder_location="",
    merchant_location="",
):
    """
    Description:
    Function allows you to read in data from the spend merchant location\
    table with default values set to 'All' where possible.
    so if you wanted to keep the defaults you would run: \
    read_retail_performance_high_streets_towns(client). However,\
    if you wanted to change
    one or more of the categorical variables you would run\
    read_retail_performance_high_streets_towns(client,\
    merchant_location_level = 'POSTAL_SECTOR').

    Args:
    - client: defined earlier in the session
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'. Choose "" for any.
    - merchant_location_level: string. Defaults to 'All'. Choose "" for any.
    - cardholder_issuing_level: string. Defaults to 'All'. Choose "" for any.
    - mcg: string. Defaults to 'All'. Choose "" for any. Choose "" for any.
    - cardholder_location: string. defaults to "" but to pick a
    specific cardholder location(s)
    run it in the following format: cardholder_location = " 'SW2' "
    or cardholder_location =" 'SW2', 'SE4' " for multiple
    - merchant_location: string. defaults to "" but to pick a specific
    merchant location(s)
    run it in the following format: merchant_location = " 'SW2' " or
    merchant_location = " 'SW2', 'SE4' " for multiple

    Returns:
    retail_performance_high_streets_towns with specification of your choice
    """

    # If we choose specific locations, we need to allow location levels
    # to be anything rather than fixed
    if cardholder_location != "":
        SQL2 = f" AND cardholder_location IN ({cardholder_location})"
        cardholder_location_level = ""
    else:
        SQL2 = ""

    if merchant_location != "":
        SQL3 = f" AND merchant_location IN ({merchant_location})"
        merchant_location_level = ""

    else:
        SQL3 = ""

    if time_period == "Quarter":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS\
        date_time, "
    elif time_period == "Month":
        SQL_date = "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),\
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS\
        date_time, "
    else:
        SQL_date = ""

    SQL1 = f"\
    SELECT {SQL_date} * FROM\
    ons-fintrans-data-prod.fintrans_visa.retail_performance_high_streets_towns\
    WHERE\
    time_period LIKE '{time_period}%' AND\
    cardholder_location_level LIKE '{cardholder_location_level}%' AND\
    merchant_location_level LIKE '{merchant_location_level}%' AND\
    mcg LIKE '{mcg}%'"

    SQL4 = " ORDER BY time_period, time_period_value"

    SQL = SQL1 + SQL2 + SQL3 + SQL4

    print(f"The SQL statement you have just selected is: {SQL}")

    df = bq.read_bq_table_sql(client, SQL)

    return df


def create_date_time(df):
    """
    Description:
    - creates date time column in pandas dataframe. irrespective of month/quarter

    Args:
    - df: pandas dataframe

    Returns:
    - df with year, month, date_time column
    """

    df["year"] = df["time_period_value"].str[:4].astype(int)

    df["month"] = np.where(
        df["time_period_value"].str[4:5] == "Q",
        3 * df["time_period_value"].str[5:6].astype(int),
        df["time_period_value"].str[4:6].astype(int),
    )

    df["date_time"] = pd.to_datetime(df["year"] * 100 + df["month"], format="%Y%m")

    # move date time to the front
    first_col = df.pop("date_time")
    df.insert(0, "date_time", first_col)

    return df


def lag(x):
    """
    Description:
    - Translates yoy/yo2y/yo3y specification into units
    based on the quarterly data

    Args:
    - yoy: "yoy" or "yo2y" or "yo3y"
    e.g. x = lag('yoy'). To use it for monthly data, x = 3 * lag('yoy').
    Case insensitive.

    Returns:
    - number of units to shift data back to obtain lag
    """
    return {
        "yoy": 4,
        "YoY": 4,
        "yo2y": 8,
        "Yo2Y": 8,
        "yo3y": 12,
        "Yo3Y": 12,
        "MoM": 1,
        "mom": 1,
        "QoQ": 1,
        "qoq": 1,
    }[x]


def get_cat_vars(table):
    """
    Description:
    - retrieves the categorical variables for each dataset based

    Args:
    - table: abbreviation or full name of table that you want the
    categorical variables of e.g. 'sml' or 'spend_merchant_location'

    Returns:
    - list of column names
    """
    return {
        "spoc": [
            "cardholder_origin",
            "cardholder_origin_country",
            "cardholder_location",
            "mcg",
            "mcc",
            "merchant_channel",
        ],
        "sml": [
            "merchant_location_level",
            "merchant_location",
            "cardholder_issuing_level",
            "cardholder_issuing_country",
            "mcg",
            "mcc",
        ],
        "rphst": [
            "cardholder_location_level",
            "cardholder_location",
            "merchant_location_level",
            "merchant_location",
            "mcg",
        ],
        "spend_origin_and_channel": [
            "cardholder_origin",
            "cardholder_origin_country",
            "cardholder_location",
            "mcg",
            "mcc",
            "merchant_channel",
        ],
        "spend_merchant_location": [
            "merchant_location_level",
            "merchant_location",
            "cardholder_issuing_level",
            "cardholder_issuing_country",
            "mcg",
            "mcc",
        ],
        "retail_performance_high_streets_towns": [
            "cardholder_location_level",
            "cardholder_location",
            "merchant_location_level",
            "merchant_location",
            "mcg",
        ],
    }[table]


def create_XoX_growth(
    df,
    time_period,
    yoy,
    categorical_vars,
    value="spend",
):
    """
    Description:
    - creates yoy/yo2y/yo3y/MoM/QoQ growth column

    Args:
    - df: Pandas dataframe
    - time_period: 'Month' or 'Quarter'
    - yoy: "yoy" or "yo2y" or "yo3y" or "MoM" or "QoQ"
    e.g. x = lag('yoy'). To use it for monthly data, lag = 3 * lag('yoy'). It has
    been designed to be case insensitive.
    - categorical_vars: list of the categorical variables in the dataset. E.g.
    if working with all of spend_merchant_location then categorical_vars =
    ['merchant_location_level','merchant_location','cardholder_issuing_level',
    'cardholder_issuing_country','mcg','mcc']. You can reduce this or change this
    if you do not have them in your dataframe.
    #- table: name/abbreviation of table e.g. 'sml'. Used as input into
    #function ## no longer used
    #to retrieve the categorical variables for a groupby. ## no longer used
    - value: str of variable you want to calculate growth of. Defaults to 'spend'.

    Returns:
    - df with lagged yoy column and yoy growth column

    Example: df = create_XoX_growth(df, 'Month', 'MoM',get_car_vars('sml'), 'spend')
    """
    if (time_period == "Month") & ~(yoy in (["MoM", "mom", "QoQ", "qoq"])):
        x = 3 * (lag(yoy))
    else:
        x = lag(yoy)
    try:
        df = df.sort_values("date_time")
    except Exception as e:
        print(f"no {e} column, sorting by time_period_value instead")
        df = df.sort_values("time_period_value")

    df[f"{value}_{yoy}_lag"] = df.groupby(categorical_vars)[value].shift(x)

    df[f"{value}_{yoy}"] = (
        100 * (df[f"{value}"] - df[f"{value}_{yoy}_lag"]) / df[f"{value}_{yoy}_lag"]
    )

    return df


def create_index(df, value, categorical_vars, index_value="t0"):
    """
    Description:
    - Creates an indexed value from the earliest date in the data

    Args:
    - df: pandas dataframe
    - value: column that we want to index e.g. 'spend'
    - categorical_vars: the categorical variables that you want to group over
    - index_value: if you want to set an index to a different time period than
    the earliest then change this value to the time_period_value/date_time value
    you want as the base for the index. Alternatively do not specify this value,
    and it will default to using the earliest available value in the dataset.


    Returns:
    - df with t0 for specified categorical variables and indexed value

    """
    if index_value == "t0":
        try:
            df_t0 = df[df["date_time"] == min(df["date_time"])]
            print(f"{value} in {min(df['date_time'])} used as base for {value} index")
        except Exception as e:
            print(
                f"\
            No {e} column, using 'time_period_value' instead. Consider\
            converting to date_time"
            )
            df = df.sort_values("time_period_value")
            df_t0 = df[df["time_period_value"] == min(df["time_period_value"])]
            print(
                f"{value} in {min(df['time_period_value'])}\
                used as base for {value} index"
            )
    else:
        try:
            df_t0 = df[df["date_time"] == index_value]
            print(f"{value} in {index_value} used as base for {value} index")
        except Exception as e:
            print(
                f"\
            No {e} column, using 'time_period_value' instead. Consider\
            converting to date_time"
            )
            df = df.sort_values("time_period_value")
            df_t0 = df[df["time_period_value"] == index_value]
            print(f"{value} in {index_value} used as base for {value} index")

    df_t0 = df_t0[categorical_vars + [value]]
    df_t0 = df_t0.rename(columns={f"{value}": f"{value}_t0"})

    df = pd.merge(df, df_t0, on=categorical_vars, how="outer")

    df[f"{value}_index"] = 100 * (df[f"{value}"] / df[f"{value}_t0"])

    return df


def create_t1_t2_growth(df, value, categorical_vars, index_value):

    """
    Description:
    - Creates an indexed value from the specified date,
    then calculates a growth rate by doing index minus 100.

    Args:
    - df: pandas dataframe
    - value: column that we want to index e.g. 'spend'
    - categorical_vars: the categorical variables that you want to group over
    - index_value: period you want as the base for the index + growth rate.

    Returns:
    - df with t0 for specified categorical variables and indexed value
    """

    df = create_index(df, value, categorical_vars, index_value)
    df[f"growth_from_{index_value}"] = df[f"{value}_index"] - 100

    return df
