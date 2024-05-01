from google.cloud import bigquery
from fintrans_toolbox.src import bq_utils as bq
import pandas as pd

client = bigquery.Client()


def read_in_data(
    table, time_period, cardholder_location_level, merchant_location_level
):
    """Create a SQL query for a given time period, cardholder location level
    and merchant location level. Read in data and convert the time_period_value to
    pd.Categorical datatype to save memory.

    Args:
        table (str): The table you wish to read the data from
            e.g. RPHST
        time_period (str): 'Month' or 'Quarter'
        cardholder_location_level (str): 'POSTAL_DISTRICT', 'POSTAL AREA'
            or 'All'
        merchant_location_level (str): 'POSTAL_DISTRICT', 'POSTAL AREA'
            or 'All'

    Returns:
        pd.DataFrame
    """
    time_period_values = ["Month", "Quarter"]
    if time_period not in time_period_values:
        raise ValueError(f"Argument time_period must be one of {time_period_values}")

    location_values = ["POSTAL_DISTRICT", "POSTAL_AREA", "All"]
    if cardholder_location_level not in location_values:
        raise ValueError(
            f"Argument cardholder_location_level must be one of {location_values}"
        )
    if merchant_location_level not in location_values:
        raise ValueError(
            f"Argument merchant_location_level must be one of {location_values}"
        )

    sql = f"""SELECT DISTINCT time_period_value,
           cardholder_location,
           merchant_location,
           spend as sum_spend
           FROM {table}
           WHERE time_period = '{time_period}'
           AND cardholder_location_level = '{cardholder_location_level}'
           AND merchant_location_level = '{merchant_location_level}'
           AND mcg = 'All'
           ORDER BY time_period_value, cardholder_location, merchant_location"""
    data = bq.read_bq_table_sql(client, sql)
    data["time_period_value"] = pd.Categorical(data["time_period_value"])

    return data


def remove_postal_areas(df):
    """Function to remove locations that have been incorrectly labelled as a
    given postal district from the dataset based on the NSPL dataset.

    Args:
        df (pd.DataFrame)

    Returns:
        pd.DataFrame
    """
    df = df.copy()

    df["numbers"] = df["merchant_location"].apply(lambda x: sum(c.isdigit() for c in x))
    postal_areas = (
        (df["merchant_location"] != "UNKNOWN")
        & (df["merchant_location"] != "All")
        & (df["numbers"] == 0)
    )
    df.loc[postal_areas, ["merchant_location"]] = "UNKNOWN"
    df = df.drop("numbers", axis=1)

    df = (
        df.groupby(
            ["time_period_value", "cardholder_location", "merchant_location"],
            observed=True,
        )["sum_spend"]
        .sum()
        .reset_index()
    )

    return df


def import_data_and_clean_locations(
    table, time_period, cardholder_location_level, merchant_location_level
):
    """Read in raw data. If importing postal district data, remove
    postal areas that have been incorrectly labelled.

    Args:
        table (str): The table you wish to read the data from
            e.g. RPHST
        time_period (str): 'Month' or 'Quarter'
        cardholder_location_level (str): 'POSTAL_DISTRICT', 'POSTAL AREA'
            or 'All'
        merchant_location_level (str): 'POSTAL_DISTRICT', 'POSTAL AREA'
            or 'All'
    Returns:
        pd.DataFrame

    """

    df = read_in_data(
        table, time_period, cardholder_location_level, merchant_location_level
    )

    if (
        cardholder_location_level == "POSTAL_DISTRICT"
        or merchant_location_level == "POSTAL_DISTRICT"
    ):
        df = remove_postal_areas(df)
        print("remove_postal_areas function used")

    return df


def calculate_unknowns_stage_1(cardholder_merchant, cardholder_all):
    """compare the data where we have cardholder location and merchant
    location to the data where we have cardholder location but merchant
    location is set to 'All'
    """

    grouped_cardholder_merchant = (
        cardholder_merchant.groupby(
            ["time_period_value", "cardholder_location"], observed=True
        )["sum_spend"]
        .sum()
        .reset_index()
    )

    cardholder_all = cardholder_all.drop(columns="merchant_location")

    unknown = cardholder_all.merge(
        grouped_cardholder_merchant,
        on=("time_period_value", "cardholder_location"),
        how="outer",
        suffixes=("_cardholder_all", "_cardholder_merchant"),
    )

    # for the case below we have data in the district_district dataframe but not the
    # district_all dataframe. so there are NO unknowns being added to the
    # district_district dataframe from the district_all data (these are cases where
    # we have more data in the district_district data).
    # here we swap the data between columns so we are keeping this data when
    # using the calculations in the following cells.
    mask = unknown["sum_spend_cardholder_all"].isnull()
    unknown.loc[
        mask, ["sum_spend_cardholder_all", "sum_spend_cardholder_merchant"]
    ] = unknown.loc[
        mask, ["sum_spend_cardholder_merchant", "sum_spend_cardholder_all"]
    ].values

    # after the swap we can fill the NaN values with '0'. so we will be subtracting
    # 0 in the calculations in the following cells.
    # We fill with a '0' because the postal area is present
    # in the district_all dataframe but not the district_district dataframe.
    # so we can say that there is '0' known spend in the district_district
    # dataframe.
    # We also have the data above, where the columns have been switched, this
    # is explained above.
    unknown["sum_spend_cardholder_merchant"] = unknown[
        "sum_spend_cardholder_merchant"
    ].fillna(0)

    unknown["sum_spend"] = (
        unknown["sum_spend_cardholder_all"] - unknown["sum_spend_cardholder_merchant"]
    )
    unknown["merchant_location"] = "UNKNOWN"

    unknown = unknown[
        ["time_period_value", "cardholder_location", "merchant_location", "sum_spend"]
    ]

    combined_data = pd.concat([cardholder_merchant, unknown], axis=0, ignore_index=True)

    combined_data = (
        combined_data.groupby(
            ["time_period_value", "cardholder_location", "merchant_location"],
            observed=True,
        )["sum_spend"]
        .sum()
        .reset_index()
    )

    return combined_data


def calculate_unknowns_stage_2(stage_1, all_all):
    """compare the dataframe created from stage 1 of the calculated unknows
    with the data where we set cardholder location to all and merchant
    location to all.
    """

    stage_1_time_grouped = (
        stage_1.groupby("time_period_value", observed=True)["sum_spend"]
        .sum()
        .reset_index()
    )

    unknowns = stage_1_time_grouped.merge(
        all_all, on="time_period_value", suffixes=("_district_district", "_all_all")
    )

    unknowns["sum_spend"] = (
        unknowns["sum_spend_all_all"] - unknowns["sum_spend_district_district"]
    )

    unknowns["cardholder_location"] = "UNKNOWN"
    unknowns["merchant_location"] = "UNKNOWN"

    unknowns = unknowns[
        ["time_period_value", "cardholder_location", "merchant_location", "sum_spend"]
    ]

    combined_data = pd.concat([stage_1, unknowns], axis=0, ignore_index=True)

    combined_data = (
        combined_data.groupby(
            ["time_period_value", "cardholder_location", "merchant_location"],
            observed=True,
        )["sum_spend"]
        .sum()
        .reset_index()
    )

    return combined_data


def calculate_unknowns_stage_3(stage_2, all_merchant):
    """compare the output of calculate unknowns stage 2 to fill in the extra
    information from the data where we have merchant location but we set
    cardholder location to 'All'
    """

    stage_2_time_grouped = (
        stage_2.groupby(["time_period_value", "merchant_location"], observed=True)[
            "sum_spend"
        ]
        .sum()
        .reset_index()
    )

    stage_2_time_grouped["cardholder_location"] = "All"

    all_merchant_merged = all_merchant.merge(
        stage_2_time_grouped,
        on=["time_period_value", "cardholder_location", "merchant_location"],
        how="outer",
        suffixes=("_all_merchant", "_stage_2"),
    )

    missing_rows = all_merchant_merged[
        all_merchant_merged["sum_spend_stage_2"].isnull()
    ]
    missing_rows_time_grouped = (
        missing_rows.groupby("time_period_value", observed=True)[
            "sum_spend_all_merchant"
        ]
        .sum()
        .reset_index()
    )

    unknown_unknown = stage_2[
        (stage_2["cardholder_location"] == "UNKNOWN")
        & (stage_2["merchant_location"] == "UNKNOWN")
    ]

    comparisson_missing_rows = unknown_unknown.merge(
        missing_rows_time_grouped,
        on=["time_period_value"],
        suffixes=("_stage_2", "_all_merchant"),
    )

    comparisson_missing_rows["sum_spend_stage_2"] = (
        comparisson_missing_rows["sum_spend"]
        - comparisson_missing_rows["sum_spend_all_merchant"]
    )

    unknown_unknown_updated = comparisson_missing_rows[
        [
            "time_period_value",
            "cardholder_location",
            "merchant_location",
            "sum_spend_stage_2",
        ]
    ].copy()
    unknown_unknown_updated.columns = unknown_unknown_updated.columns.str.replace(
        "_stage_2", ""
    )

    stage_2_unknown_updated = stage_2.merge(
        unknown_unknown_updated,
        on=["time_period_value", "cardholder_location", "merchant_location"],
        how="outer",
        suffixes=("_original", "_updated"),
    )

    stage_2_unknown_updated["sum_spend"] = stage_2_unknown_updated[
        "sum_spend_updated"
    ].fillna(stage_2_unknown_updated["sum_spend_original"])

    stage_2_unknown_updated.drop(
        columns=["sum_spend_original", "sum_spend_updated"], inplace=True
    )

    new_unknown_merchant = missing_rows[
        [
            "time_period_value",
            "cardholder_location",
            "merchant_location",
            "sum_spend_all_merchant",
        ]
    ].copy()

    new_unknown_merchant["cardholder_location"] = "UNKNOWN"
    new_unknown_merchant = new_unknown_merchant.rename(
        columns={"sum_spend_all_merchant": "sum_spend"}
    )

    complete_dataset = pd.concat(
        [stage_2_unknown_updated, new_unknown_merchant], axis=0, ignore_index=True
    )

    return complete_dataset


def calculate_unknowns(cardholder_district, cardholder_all, all_merchant, all_all):
    """Run all the stages of the unknown calculations to fill in all the missing
    unknown unknowns.
    """

    stage_1 = calculate_unknowns_stage_1(cardholder_district, cardholder_all)
    stage_2 = calculate_unknowns_stage_2(stage_1, all_all)
    complete_dataset = calculate_unknowns_stage_3(stage_2, all_merchant)

    return complete_dataset


def create_complete_dataset(table, location_level, time_period):

    all_all = import_data_and_clean_locations(
        table=table,
        time_period=time_period,
        cardholder_location_level="All",
        merchant_location_level="All",
    )
    all_merchant = import_data_and_clean_locations(
        table=table,
        time_period=time_period,
        cardholder_location_level="All",
        merchant_location_level=location_level,
    )
    cardholder_all = import_data_and_clean_locations(
        table=table,
        time_period=time_period,
        cardholder_location_level=location_level,
        merchant_location_level="All",
    )
    cardholder_merchant = import_data_and_clean_locations(
        table=table,
        time_period=time_period,
        cardholder_location_level=location_level,
        merchant_location_level=location_level,
    )

    complete_dataset = calculate_unknowns(
        cardholder_merchant, cardholder_all, all_merchant, all_all
    )

    return complete_dataset
