from google.cloud import bigquery
import time

print("Start a big query client to use these functions")


def read_full_bq_table(client, table_id):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       table_id (str): the table id to determine what table to return
       e.g "ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"
    Returns:
       the query results in a Pandas dataframe , or None if error
    """

    sql = "SELECT * FROM " + table_id
    start = time.time()
    try:
        # client = bigquery.Client()
        df = client.query(sql).to_dataframe()
        # View table properties
        print("Table has {} dimensions".format(df.shape))
        end = time.time()
        print(f"Table took {round(end - start)} seconds to load")
        return df
    except Exception as e:
        print(f"Error getting data {e}")
        return None


def read_bq_table_sql(client, sql):
    """
    Gets data from BigQuery and saves to Pandas DataFrame

    Args:
       sql (str): the sql query to determine what data to return
       e.g "SELECT * FROM ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"
    Returns:
       the query results in a Pandas dataframe , or None if error
    """
    start = time.time()
    try:
        # client = bigquery.Client()
        df = client.query(sql).to_dataframe()
        # View table properties
        print("Table has {} dimensions".format(df.shape))
        end = time.time()
        print(f"Table took {round(end - start)} seconds to load")
        return df
    except Exception as e:
        print(f"Error getting data {e}")
        return None


def view_table_properties(client, table_id):
    """
    Gets table from BigQuery and returns the properties of the table

    Args:
       table_id (str): the full id of the tables location
       ('project_id.dataset_id.table_id')
       e.g "ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"
    Returns:
       Properties of the table in big query. It does not return the dataframe.
    """
    # Construct a BigQuery client object.
    # client = bigquery.Client()
    try:
        table = client.get_table(table_id)  # Make an API request.

        # View table properties
        print(
            "Got table '{}.{}.{}'.".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        print("Table was created: {}".format(table.created))
        print("Table was modified: {}".format(table.modified))
        print("Table was created using query:{}".format(table.view_query))
        print("Table has {} rows".format(table.num_rows))
        print("Table is {} bytes".format(table.num_bytes))
        print()
        print("Table description: {}".format(table.description))
        print()
        print("Table schema: {}".format(table.schema))
    except Exception as e:
        print(f"Error printing table properties: {e}")


def list_bq_tables(client, dataset_id="ons-fintrans-analysis-prod.fin_wip_notebook"):
    """
    Lists tables from BigQuery from the dataset_id mentioned. The default dataset_id
    is 'ons-fintrans-analysis-prod.fin_wip_notebook' but this can be changed.

    Args:
       dataset_id (str): the full id of the dataset location ('project_id.dataset_id')
       e.g "ons-fintrans-analysis-prod.fin_wip_notebook"
    Returns:
       List of the tables within a dataset.
    """

    # Construct a BigQuery client object.
    # client = bigquery.Client()

    # TODO(developer): Set dataset_id to the ID of the dataset to fetch.
    # dataset_id = 'your-project.your_dataset'

    dataset = client.get_dataset(dataset_id)  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    friendly_name = dataset.friendly_name
    print(
        "Got dataset '{}' with friendly_name '{}'.".format(
            full_dataset_id, friendly_name
        )
    )

    # View dataset properties.
    print("Description: {}".format(dataset.description))
    print("Labels:")
    labels = dataset.labels
    if labels:
        for label, value in labels.items():
            print("\t{}: {}".format(label, value))
    else:
        print("\tDataset has no labels defined.")

    # View tables in dataset.
    print("Tables:")
    tables = list(client.list_tables(dataset))  # Make an API request(s).
    if tables:
        for table in tables:
            print("\t{}".format(table.table_id))
    else:
        print("\tThis dataset does not contain any tables.")


def list_bq_datasets(client):
    """
    Lists datasets available from BigQuery from project-id 'ons-fintrans-analysis-prod'

    Returns:
       List of datasets within a project.
    """
    # Construct a BigQuery client object.
    # client = bigquery.Client()

    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:
            print("\t{}".format(dataset.dataset_id))
    else:
        print("{} project does not contain any datasets.".format(project))


def delete_table(client, table_id):
    """
    Deletes tables from BigQuery from the table_id mentioned.

    Extra step to confirm you want to delete the table through user input

    Args:
       table_id (str):
       the full id of the dataset location ('project_id.dataset_id.table_id')
       e.g "ons-fintrans-analysis-prod.fin_wip_notebook.harry_test"

    """
    # Construct a BigQuery client object.
    # client = bigquery.Client()
    # TODO(developer): Set table_id to the ID of the table to fetch.
    # table_id = 'your-project.your_dataset.your_table'
    print(
        "Confirm deletion of {} by typing delete into the user input".format(table_id)
    )
    user_input = input("Enter delete:")

    if user_input == "delete":
        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        try:
            client.delete_table(table_id)
            print("Deleted table '{}'.".format(table_id))
        except Exception as e:
            print("Table '{}' not found, check ID".format(table_id))
            print(f"Error: {e}")
    else:
        print("Type delete to confirm you want to delete the table {}".format(table_id))


def estimate_costs(sql):
    """
    Estimates cost for running a job with Big Query.
    By setting job_config.dry_run = True
    we do not actually run the job.

    The cost estimate is based off of US pricing:
    (https://stackoverflow.com/questions/58561153/what-is-the
    -python-api-i- can-use-to-calculate-the-cost-of-a-bigquery-query)

    Args:
       SQL statement that you want to run
    Returns:
       Amount of bytes to be processed and estimated cost
    """
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job_config.use_query_cache = False
    query_job = bigquery.Client().query(
        (sql),
        location="europe-west2",  # or wherever your data is
        job_config=job_config,
    )
    cost_dollars = (query_job.total_bytes_processed / 1024**4) * 5
    print(
        "{} bytes will be processed at and estimated cost of ${}".format(
            query_job.total_bytes_processed, cost_dollars
        )
    )


def boundary_file_download(client, postal_level, output_location):
    """
    Gets boundary files from
    ons-fintrans-data-prod-fintrans-reference-des-ingress bucket

    Args:
       postal_level (str): either sector or district depending on the
       level you wish to be available locally
       output_location (str): where you would like to boundary files to be saved
    Returns:
       The boundary files saved locally
    """

    # All files needed
    list_of_files = [".cpg", ".dbf", ".prj", ".qmd", ".shp", ".shx"]

    bucket = client.get_bucket("ons-fintrans-data-prod-fintrans-reference-des-ingress")

    for i in list(list_of_files):
        output_file = output_location + postal_level + i
        bucket_path = (
            "ons-des-prod-fintrans-reference-ingress/fintrans_reference/geodata/"
        )
        bucket_path = bucket_path + postal_level + "/" + postal_level + i
        blob = bucket.blob(bucket_path)
        blob.download_to_filename(output_file)
