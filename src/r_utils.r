library(bigrquery)
library(dplyr)
library(DBI)

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

  print(sql)
  try(df <- dbGetQuery(con, sql))
  return(df)
}

read_spend_merchant_location <- function(
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
