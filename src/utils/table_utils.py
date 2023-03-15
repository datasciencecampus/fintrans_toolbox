from fintrans_toolbox.src.utils import bq_utils as bq


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

    SQL1 = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_merchant_location\
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
    - cardholder_origin_country: string. Defaults to 'All'
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

    if destination_country != "":
        SQL3 = f" AND destination_country IN ({destination_country})"
    else:
        SQL3 = ""

    SQL1 = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel\
        WHERE\
        time_period LIKE '{time_period}%' AND\
        cardholder_origin LIKE '{cardholder_origin}%' AND\
        cardholder_origin_country LIKE '{cardholder_origin_country}%' AND\
        mcg LIKE '{mcg}%' AND\
        mcc LIKE '{mcc}' AND\
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

    SQL1 = f"\
    SELECT * FROM\
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
