library(bigrquery)
library(dplyr)
library(geosphere)
library(data.table)
library(DBI)
library(magrittr)
utils::globalVariables(".")
globalVariables(c("postcode_sector", "postcode_district", "postcode_area"))
globalVariables(c(":=", "!!", "..value"))
globalVariables(names(data))

print("Establish a connection with Big Query via dbConnect
before using these functions")

connect_bigquery <- function() {
  con <- dbConnect(
    bigquery(),
    project = "ons-fintrans-analysis-prod"
  )

  return(con)
}

read_full_bq_table <- function(con, table_id) {
  #' Gets data from BigQuery and saves to Pandas DataFrame
  #'
  #'  Args:
  #'   table_id (str): the table id to determine what table to return
  #'    e.g "ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"
  #' Returns:
  #'    the query results in a Pandas dataframe , or None if error
  #' Example:
  #' read_full_bq_table(con,ons-fintrans-analysis-prod.fin_wip_notebook.harry_test)


  sql <- paste0("SELECT * FROM ", table_id)
  print(sql)
  try(df <- dbGetQuery(con, sql))
  return(df)
}


read_bq_table_sql <- function(con, sql) {
  #' Gets data from BigQuery and saves to Pandas DataFrame
  #'
  #'  Args:
  #'   table_id (str): the table id to determine what table to return
  #'    e.g "ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"
  #' Returns:
  #'    the query results in a Pandas dataframe , or None if error
  #' Example:
  #' SQL <- "SELECT * FROM ..."
  #' read_bq_table_sql(con,sql)

  print(sql)
  try(df <- dbGetQuery(con, sql))
  return(df)
}

read_spend_merchant_location <- function(
    #
    #   Description:
    #   Function allows you to read in data from the spend merchant location table\
    #   with default values set to 'All' where possible.
    #   so if you wanted to keep the defaults you would run:\
    #   read_spend_merchant_location(client). However, if you wanted to change
    #   one or more of the categorical variables you would run\
    #   read_spend_merchant_location(client, merchant_location_level = 'POSTAL_SECTOR').\
    #   If you want a value to cover anything then define it as an empty string e.g.\
    #   mcg = ''
    #
    #   Args:
    #  - client: defined earlier in the session
    #  - time_period: 'Month' or 'Quarter'. Defaults to 'All'
    #  - merchant_location_level: string. Defaults to 'All'
    #  - cardholder_issuing_level: string. Defaults to 'All'
    #  - mcg: string. Defaults to 'All'
    #  - mcc: string. Defaults to 'All'
    #  - merchant_location: string. defaults to '' so that all
    #  merchant locations are picked up
    #  - cardholder_issuing_country: string. defaults to '' so that all cardholder
    #  issuing countries are picked up.
    #
    #  Returns:
    #    Spend Merchant Location with specification of your choice
    # Example
    # read_spend_merchant_location(con, time_period = "Month", merchant_location_level = "All", cardholder_issuing_level = "All", mcg = "All", mcc = "All, #"merchant_location = "PO", cardholder_issuing_country = "")
    con,
    time_period = "Quarter",
    merchant_location_level = "All",
    cardholder_issuing_level = "All",
    mcg = "All",
    mcc = "All",
    merchant_location = "",
    cardholder_issuing_country = "") {
  if (merchant_location != "") {
    sql2 <- paste0(" AND merchant_location IN (", merchant_location, ")")
    merchant_location_level <- ""
  } else {
    sql2 <- ""
  }

  if (cardholder_issuing_country != "") {
    sql3 <- paste0(
      " AND cardholder_issuing_country IN (",
      cardholder_issuing_country, ")"
    )
    cardholder_issuing_level <- ""
  } else {
    sql3 <- ""
  }
  if (time_period == "Quarter") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS
        date_time, "
  } else if (time_period == "Month") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS
        date_time, "
  } else {
    sql_date <- " "
  }

  sql1 <- paste0("
        SELECT ", sql_date, " * , FROM
        ons-fintrans-data-prod.fintrans_visa.spend_merchant_location
        WHERE
        time_period LIKE '", time_period, "%' AND
        cardholder_issuing_level LIKE '", cardholder_issuing_level, "%' AND
        merchant_location_level LIKE '", merchant_location_level, "%' AND
        mcg LIKE '", mcg, "%' AND
        mcc LIKE '", mcc, "%'")

  sql4 <- " ORDER BY time_period, time_period_value"

  sql <- paste0(sql1, sql2, sql3, sql4)

  print(paste0("The SQL statement you have just selected is: ", sql))

  df <- read_bq_table_sql(con, sql)

  return(df)
}



read_spend_origin_and_channel <- function(
    con,
    time_period = "Quarter",
    cardholder_origin = "All",
    cardholder_origin_country = "All",
    mcg = "All",
    merchant_channel = "All",
    mcc = "All",
    cardholder_location = "",
    destination_country = "") {
  #' Description:
  #' Function allows you to read in data from the spend_origin_and_channel\
  #' table with default values set to 'All' where possible.
  #' so if you wanted to keep the defaults you would run:\
  #' read_spend_origin_and_channel(client). However, if you wanted to change
  #' one or more of the categorical variables you would run\
  #' read_spend_origin_and_channel(client, merchant_channel = 'Face to Face').\
  #' If you want a value to cover anything then define it as an empty string
  # e.g.  mcg = ''

  #' Args:
  #' - client: defined earlier in the session
  #- time_period: 'Month' or 'Quarter'. Defaults to 'All'
  #- cardholder_origin: string. Defaults to 'All'
  #- cardholder_origin_country: string. Defaults to 'All'. If not 'All',
  # it changes cardholder origin to be ''.
  #- mcg: string. Defaults to 'All'
  #- mcc: boolean. Defaults to True. If False it will gather all mccs
  # under the MCG specified.
  #- merchant_channel: string. Defaults to 'All'
  #- cardholder_location: string. defaults to '', meaning it can take any value.
  # To pick a specific cardholder location(s)
  # run it in the following format: cardholder_location = " 'SW2' " or
  # cardholder_location = " 'SW2', 'SE4' " for multiple
  #- destination_country: string. defaults to '', meaning it can take
  #  any value.
  # to pick a specific destination country(s)
  # run it in the following format: destination_country = " 'UNITED KINGDOM' "
  # or destination_country = " 'UNITED KINGDOM', 'FRANCE' " for multiple

  # Returns:
  # Spend Merchant Location with specification of your choice


  if (cardholder_location != "") {
    sql2 <- paste0(" AND cardholder_location IN (", cardholder_location, ")")
    cardholder_origin <- ""
    cardholder_origin_country <- ""
  } else {
    sql2 <- ""
  }
  if (cardholder_origin_country != "All") {
    cardholder_origin <- ""
  }
  if (destination_country != "") {
    sql3 <- paste0(" AND destination_country IN (", destination_country, ")")
  } else {
    sql3 <- ""
  }
  if (time_period == "Quarter") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS
        date_time, "
  } else if (time_period == "Month") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS
        date_time, "
  } else {
    sql_date <- " "
  }
  sql1 <- paste0("
        SELECT ", sql_date, " * FROM
        ons-fintrans-data-prod.fintrans_visa.spend_origin_and_channel
        WHERE
        time_period LIKE '", time_period, "%' AND
        cardholder_origin LIKE '", cardholder_origin, "%' AND
        cardholder_origin_country LIKE '", cardholder_origin_country, "%' AND
        mcg LIKE '", mcg, "%' AND
        mcc LIKE '", mcc, "%' AND
        merchant_channel LIKE '", merchant_channel, "%'")

  sql4 <- " ORDER BY time_period, time_period_value"

  sql <- paste0(sql1, sql2, sql3, sql4)
  print(paste0("The SQL statement you have just selected is: ", sql))
  df <- read_bq_table_sql(con, sql)

  return(df)
}


read_retail_performance <- function(
    client,
    time_period = "Quarter",
    cardholder_location_level = "All",
    merchant_location_level = "All",
    mcg = "All",
    cardholder_location = "",
    merchant_location = "") {
  # Description:
  # Function allows you to read in data from the spend merchant location\
  # table with default values set to 'All' where possible.
  # so if you wanted to keep the defaults you would run: \
  # read_retail_performance_high_streets_towns(client). However,\
  # if you wanted to change
  # one or more of the categorical variables you would run\
  # read_retail_performance_high_streets_towns(client,\
  # merchant_location_level = 'POSTAL_SECTOR').

  # Args:
  #- client: defined earlier in the session
  #- time_period: 'Month' or 'Quarter'. Defaults to 'All'. Choose "" for any.
  #- merchant_location_level: string. Defaults to 'All'. Choose "" for any.
  #- cardholder_issuing_level: string. Defaults to 'All'. Choose "" for any.
  #- mcg: string. Defaults to 'All'. Choose "" for any. Choose "" for any.
  #- cardholder_location: string. defaults to "" but to pick a
  # specific cardholder location(s)
  # run it in the following format: cardholder_location = " 'SW2' "
  # or cardholder_location =" 'SW2', 'SE4' " for multiple
  #- merchant_location: string. defaults to "" but to pick a specific
  # merchant location(s)
  # run it in the following format: merchant_location = " 'SW2' " or
  # merchant_location = " 'SW2', 'SE4' " for multiple

  # Returns:
  # retail_performance_high_streets_towns with specification of your choice


  # If we choose specific locations, we need to allow location levels
  # to be anything rather than fixed
  if (cardholder_location != "") {
    sql2 <- paste0(" AND cardholder_location IN (", cardholder_location, ")")
    cardholder_location_level <- ""
  } else {
    sql2 <- ""
  }
  if (merchant_location != "") {
    sql <- paste0(" AND merchant_location IN (", merchant_location, ")")
    merchant_location_level <- ""
  } else {
    sql3 <- ""
  }
  if (time_period == "Quarter") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,6,6)AS int)*3, 01,00,00,00) AS
        date_time, "
  } else if (time_period == "Month") {
    sql_date <- "DATETIME( CAST(SUBSTRING(time_period_value, 1,4) AS int),
        CAST(SUBSTRING(time_period_value,5,6)AS int), 01,00,00,00) AS
        date_time, "
  } else {
    sql_date <- ""
  }
  sql1 <- paste0("
    SELECT ", sql_date, " * FROM
    ons-fintrans-data-prod.fintrans_visa.retail_performance_high_streets_towns
    WHERE
    time_period LIKE '", time_period, "%' AND
    cardholder_location_level LIKE '", cardholder_location_level, "%' AND
    merchant_location_level LIKE '", merchant_location_level, "%' AND
    mcg LIKE '", mcg, "%'")

  sql4 <- " ORDER BY time_period, time_period_value"

  sql <- paste0(sql1, sql2, sql3, sql4)

  print(paste0("The SQL statement you have just selected is: ", sql))

  df <- read_bq_table_sql(client, sql)

  return(df)
}


lag_define <- function(x) {
  # Description:
  #- Translates yoy/yo2y/yo3y specification into units
  # based on the quarterly data

  # Args:
  #- yoy: "yoy" or "yo2y" or "yo3y"
  # e.g. x = lag('yoy'). To use it for monthly data, x = 3 * lag('yoy').
  # Case insensitive.

  # Returns:
  #- number of units to shift data back to obtain lag
  # example
  # lag_define("yoy")
  switch(x,
    "yoy" = 4,
    "YoY" = 4,
    "yo2y" = 8,
    "Yo2Y" = 8,
    "yo3y" = 12,
    "Yo3Y" = 12,
    "MoM" = 1,
    "mom" = 1,
    "QoQ" = 1,
    "qoq" = 1
  )
}


get_cat_vars <- function(table) {
  # Define a dictionary of categorical variables for each table
  # Example
  # get_cat_vars("spoc")
  cat_vars <- list(
    spoc = c(
      "cardholder_origin",
      "cardholder_origin_country",
      "cardholder_location",
      "mcg",
      "mcc",
      "merchant_channel"
    ),
    sml = c(
      "merchant_location_level",
      "merchant_location",
      "cardholder_issuing_level",
      "cardholder_issuing_country",
      "mcg",
      "mcc"
    ),
    rphst = c(
      "cardholder_location_level",
      "cardholder_location",
      "merchant_location_level",
      "merchant_location",
      "mcg"
    ),
    spend_origin_and_channel = c(
      "cardholder_origin",
      "cardholder_origin_country",
      "cardholder_location",
      "mcg",
      "mcc",
      "merchant_channel"
    ),
    spend_merchant_location = c(
      "merchant_location_level",
      "merchant_location",
      "cardholder_issuing_level",
      "cardholder_issuing_country",
      "mcg",
      "mcc"
    ),
    retail_performance_high_streets_towns = c(
      "cardholder_location_level",
      "cardholder_location",
      "merchant_location_level",
      "merchant_location",
      "mcg"
    )
  )

  # Return the categorical variables of the specified table
  return(cat_vars[[table]])
}


create_xox_growth <- function(
    #
    #    Description:
    #    - creates yoy/yo2y/yo3y/MoM/QoQ growth column
    #    Args:
    #    - df: Pandas dataframe
    #    - time_period: 'Month' or 'Quarter'
    #    - yoy: "yoy" or "yo2y" or "yo3y" or "MoM" or "QoQ"
    #    e.g. x = lag('yoy'). To use it for monthly data, lag = 3 * lag('yoy'). It has
    #    been designed to be case insensitive.
    #    - categorical_vars: list of the categorical variables in the dataset. E.g.
    #    if working with all of spend_merchant_location then categorical_vars =
    #    ['merchant_location_level','merchant_location','cardholder_issuing_level',
    #    'cardholder_issuing_country','mcg','mcc']. You can reduce this or change this
    #    if you do not have them in your dataframe.
    #- table: name/abbreviation of table e.g. 'sml'. Used as input into
    # function ## no longer used
    # to retrieve the categorical variables for a groupby. ## no longer used
    #    - value: str of variable you want to calculate growth of. Defaults to 'spend'.
    #
    #    Returns:
    #    - df with lagged yoy column and yoy growth column
    #
    #    Example: df = create_XoX_growth(df, 'Month', 'MoM',get_car_vars('sml'), 'spend')
    df,
    time_period,
    yoy,
    categorical_vars,
    value = "spend") {
  if (time_period == "Month" && !(yoy %in% c("MoM", "mom", "QoQ", "qoq"))) {
    x <- 3 * lag_define(yoy)
  } else {
    x <- lag_define(yoy)
  }

  if ("date_time" %in% colnames(df)) {
    df <- df[order(df$date_time), ]
  } else {
    print("no date_time column, sorting by time_period_value instead")
    df <- df[order(df$time_period_value), ]
  }

  df <- as.data.table(df)
  df <- df[, paste0(value, "_", yoy, "_lag") := shift(get(value), n = x),
    by = categorical_vars
  ]
  df <- df[, paste0(value, "_", yoy) :=
    (100 * (get(value) - get(paste0(value, "_", yoy, "_lag")))
      / get(paste0(value, "_", yoy, "_lag")))]

  return(df)
}

create_index <- function(df,
                         value,
                         categorical_vars,
                         index_value = "t0") {
  if (index_value == "t0") {
    tryCatch(
      {
        df_t0 <- df[df$date_time == min(df$date_time), ]
        print(paste0(
          value, " in ", min(df$date_time),
          " used as base for ", value, " index"
        ))
      },
      error = function(e) {
        print(paste0(
          "No ", e,
          " column, using 'time_period_value' instead.
Consider converting to date_time"
        ))
        df <- df[order(df$time_period_value), ]
        df_t0 <- df[df$time_period_value ==
          min(df$time_period_value), ]
        head(df_t0)
        print(paste0(
          value, " in ", min(df$time_period_value),
          " used as base for ", value, " index"
        ))
      }
    )
  } else {
    tryCatch(
      {
        df_t0 <- df[df$date_time == index_value, ]
        print(paste0(
          value, " in ", index_value,
          " used as base for ", value, " index"
        ))
      },
      error = function(e) {
        print(paste0(
          "No ", e,
          " column, using 'time_period_value' instead.
Consider converting to date_time"
        ))
        df <- df[order(df$time_period_value), ]
        df_t0 <- df[df$time_period_value == index_value, ]
        head(df_t0)
        print(paste0(
          value, " in ", index_value,
          " used as base for ", value, " index"
        ))
      }
    )
  }



  # get df of all cat vals and value "spend"
  df_t0 <- df_t0[, c(categorical_vars, value), ]
  setnames(df_t0, value, paste0(value, "_t0"))

  df_t0 <- as.data.table(df_t0)
  df <- as.data.table(df)


  df <- merge(df, df_t0, by = categorical_vars, all = TRUE)

  df[, paste0(value, "_index")] <- (100 * (df[, ..value] /
    df[, paste0(..value, "_t0")]))


  print(head(df))
  return(df)
}

calculate_distance_from_point <- function(data, input, wanted) {
  # becasue pre-commit hooks hates data.table (i want to cry)
  postcode_sector <- postcode_district <- postcode_area <- NULL

  # filtering for input postcode to get long/lat points
  postcode_coord <- data[postcode_sector == input |
    postcode_district == input | postcode_area == input, ]

  if (input %in% postcode_coord$postcode_area) {
    idx <- which(postcode_coord$postcode_area == input)[1]
    lat <- postcode_coord$area_lat[idx]
    long <- postcode_coord$area_long[idx]
  } else if (input %in% postcode_coord$postcode_district) {
    idx <- which(postcode_coord$postcode_district == input)[1]
    lat <- postcode_coord$district_lat[idx]
    long <- postcode_coord$district_long[idx]
  } else if (input %in% postcode_coord$postcode_sector) {
    idx <- which(postcode_coord$postcode_sector == input)[1]
    lat <- postcode_coord$sector_lat[idx]
    long <- postcode_coord$sector_long[idx]
  } else {
    print("Not found")
    lat <- NA
    long <- NA
  }

  # Once gotten long/lat points, creating distance between input desired
  # point and all other points wanted
  if (wanted == "sector") {
    data <- data[, ":="(dist_sector = distHaversine(
      c(long, lat),
      cbind(data$sector_long, data$sector_lat)
    ))]
  } else if (wanted == "district") {
    data <- data[, ":="(dist_district = distHaversine(
      c(long, lat),
      cbind(data$district_long, data$district_lat)
    ))]
  } else if (wanted == "area") {
    data <- data[, ":="(dist_area = distHaversine(
      c(long, lat),
      cbind(data$area_long, data$area_lat)
    ))]
  } else {
    print("please enter 'sector' , 'district' or 'area'")
  }

  return(data)
}
