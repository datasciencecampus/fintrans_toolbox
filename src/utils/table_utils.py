import bq_utils as bq


def read_spend_merchant_location(
    client,
    time_period="Quarter",
    merchant_location_level="All",
    cardholder_issuing_level="All",
    mcg="All",
    mcc_all=True,
):
    """
    Function allows you to read in data from the spend merchant location table\
    with default values set to 'All' where possible.
    so if you wanted to keep the defaults you would run:\
    read_spend_merchant_location(client). However, if you wanted to change
    one or more of the categorical variables you would run\
    read_spend_merchant_location(client, merchant_location_level = 'POSTAL_SECTOR').\
    The final default variable sets mcc_all to False. It assumes that you want\
    to filter for where MCC = 'All'. If set to False it will allow
    for all values of MCC to be selected.

    Args:
    - client: defined earlier in the session
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    - merchant_location_level: string. Defaults to 'All'
    - cardholder_issuing_level: string. Defaults to 'All'
    - mcg: string. Defaults to 'All'
    - mcc_all: boolean. Defaults to True. If False it will gather all mccs
    under the MCG specified.

    Returns:
       Spend Merchant Location with specification of your choice
    """
    if mcc_all:
        SQL = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_merchant_location\
        WHERE\
        time_period = '{time_period}' AND\
        cardholder_issuing_level = '{cardholder_issuing_level}' AND\
        merchant_location_level = '{merchant_location_level}' AND\
        mcg = '{mcg}' AND\
        mcc = 'All'\
        ORDER BY time_period, time_period_value"
    else:
        SQL = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_merchant_location\
        WHERE\
        time_period = '{time_period}' AND\
        cardholder_issuing_level = '{cardholder_issuing_level}' AND\
        merchant_location_level = '{merchant_location_level}' AND\
        mcg = '{mcg}'\
        ORDER BY time_period, time_period_value"

    df = bq.read_bq_table_sql(client, SQL)

    return df


def read_spend_origin_and_channel(
    client,
    time_period="Quarter",
    cardholder_origin="All",
    cardholder_origin_country="All",
    mcg="All",
    merchant_channel="All",
    mcc_all=True,
):
    """
    Function allows you to read in data from the spend_origin_and_channel\
    table with default values set to 'All' where possible.
    so if you wanted to keep the defaults you would run:\
    read_spend_origin_and_channel(client). However, if you wanted to change
    one or more of the categorical variables you would run\
    read_spend_origin_and_channel(client, merchant_channel = 'Face to Face').\
    The final default variable sets mcc_all to False. It assumes that you\
    want to filter for where MCC = 'All'. If set to False it will allow
    for all values of MCC to be selected.

    Args:
    - client: defined earlier in the session
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    - cardholder_origin: string. Defaults to 'All'
    - cardholder_origin_country: string. Defaults to 'All'
    - mcg: string. Defaults to 'All'
    - mcc_all: boolean. Defaults to True. If False it will gather all mccs
    under the MCG specified.
    - merchant_channel: string. Defaults to 'All'

    Returns:
       Spend Merchant Location with specification of your choice
    """
    if mcc_all:
        SQL = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel\
        WHERE\
        time_period = '{time_period}' AND\
        cardholder_origin = '{cardholder_origin}' AND\
        cardholder_origin_country = '{cardholder_origin_country}' AND\
        mcg = '{mcg}' AND\
        mcc = 'All' AND\
        merchant_channel = 'All'\
        ORDER BY time_period, time_period_value"
    else:
        SQL = f"\
        SELECT * FROM ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel\
        WHERE\
        time_period = '{time_period}' AND\
        cardholder_origin = '{cardholder_origin}' AND\
        cardholder_origin_country = '{cardholder_origin_country}' AND\
        mcg = '{mcg}' AND\
        merchant_channel = 'All'\
        ORDER BY time_period, time_period_value"

    df = bq.read_bq_table_sql(client, SQL)

    print(
        "Warning: There is no default value for 'destination_country' \
    because there is no 'All'value for this categorical variable.\
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
):
    """
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
    - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    - merchant_location_level: string. Defaults to 'All'
    - cardholder_issuing_level: string. Defaults to 'All'
    - mcg: string. Defaults to 'All'


    Returns:
    retail_performance_high_streets_towns with specification of your choice
    """

    SQL = f"\
    SELECT * FROM\
    ons-fintrans-data-prod.fintrans_visa.retail_performance_high_streets_towns\
    WHERE\
    time_period = '{time_period}' AND\
    cardholder_location_level = '{cardholder_location_level}' AND\
    merchant_location_level = '{merchant_location_level}' AND\
    mcg = '{mcg}'\
    ORDER BY time_period, time_period_value"

    df = bq.read_bq_table_sql(client, SQL)

    return df
