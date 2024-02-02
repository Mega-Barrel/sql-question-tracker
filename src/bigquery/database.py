""" Big Query class for handling table CURD operations"""

# Internal packages
import os
from time import sleep

# Installed packages
from dotenv import load_dotenv
from google.cloud import bigquery   #pylint: disable=E0401
from google.oauth2 import service_account   #pylint: disable=E0401

# Exceptions import
from google.cloud.exceptions import NotFound    #pylint: disable=E0401

class BigQueryOperations:
    """Encapsulates operations for interacting with BigQuery tables."""

    def __init__(self, dataset_name, table_name) -> None:
        """Initializes the BigQuery client and dataset ID.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            credentials_path (str): The path to the service account JSON file.
        """
        # load .env file
        load_dotenv()
        # Load environment variables
        self._project_id = os.environ.get("project_id")
        self._dataset_name = dataset_name
        self._table_name = table_name
        self._table_id = f"{self._project_id}.{self._dataset_name}.{self._table_name}"
        self.credentials = service_account.Credentials.from_service_account_file(
            "bq_service_account.json"
        )
        # Creating bigquery client
        self._client = bigquery.Client(
            credentials=self.credentials,
            project=self._project_id
        )
        # Create empty list
        self._schema = []

        # Perform checks to make sure dataset, table exist.
        dataset_exist = self.dataset_exists()
        if dataset_exist:
            table_exist = self.table_exists()
            if table_exist:
                self.create_partitioned_table()

    def dataset_exists(self) -> None:
        """Checks if a dataset exists in the project.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            None: Creates dataset if it does not exists.
        """
        # Set dataset_id to the ID of the dataset to determine existence.
        # dataset_id = "your-project.your_dataset"
        dataset_id = f"{self._project_id}.{self._dataset_name}"
        try:
            self._client.get_dataset(dataset_id)  # Make an API request.
            print(f"Dataset {self._dataset_name} already exists.")
            return False
        except NotFound:
            print(f"Dataset {self._dataset_name} does not exist.")
            print(f"Creating new dataset with name: {self._dataset_name}.")
            # Construct a full Dataset object to send to the API.
            dataset = bigquery.Dataset(dataset_id)
            # Specify the geographic location where the dataset should reside.
            dataset.location = "asia-south1"
            # Send the dataset to the API for creation, with an explicit timeout.
            # Raises google.api_core.exceptions.Conflict if the Dataset already
            # exists within the project.
            dataset = self._client.create_dataset(dataset, timeout=30)  # Make an API request.
            print(f"Created dataset {dataset_id}.")
            print('Sleeping for 1 seconds.')
            sleep(1)
            return True

    def table_exists(self):
        """Checks if a table exists in the dataset.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            self._client.get_table(self._table_id)    # Make an API request.
            print(f"Table {self._table_id} already exists.")
            return False
        except NotFound:
            print(f"Table `{self._table_name}` does not exists.")
            return True

    def create_partitioned_table(self) -> None:
        """Creates a new table in the specified dataset, with custom schema and Column Partition.
        
        Output:
            Creates BQ table with custom schema
        """
        # Use format "your-project.your_dataset.your_table_name" for table_id
        schema = [
            bigquery.SchemaField(
                "page_id", 
                field_type="STRING",
                description="Notion Page ID"
            ),
            bigquery.SchemaField(
                "question_title", 
                "STRING", 
                description="Question Title"
            ),
            bigquery.SchemaField(
                "difficulty", 
                "STRING", 
                description="Question Difficulty (Eg: Easy, Medium, Hard)"
            ),
            bigquery.SchemaField(
                "created_at", 
                "TIMESTAMP", 
                description="Question CreatedAt"
            ),
            bigquery.SchemaField(
                "platform", 
                "STRING", 
                description="Question Platform (Eg: leetcode, Interview Bit, etc.)"
            ),
            bigquery.SchemaField(
                "company", 
                "STRING", 
                description="Company (Eg: Amazon, Meta, etc.)"
            ),
            bigquery.SchemaField(
                "question_type", 
                "STRING", 
                description="Question Type (Eg: SQL, Pandas, DSA)"
            ),
            bigquery.SchemaField(
                "question_link", 
                "STRING", description="Question URL"),
            bigquery.SchemaField(
                "question_status", 
                "STRING", description="Question Status (Solved, UnSolved)"
            ),
            bigquery.SchemaField(
                "page_url", 
                "STRING", description="Notion Page URL"
            )
        ]

        table = bigquery.Table(self._table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_= bigquery.TimePartitioningType.DAY,
            field = "created_at",  # name of column to use for partitioning
        )

        table = self._client.create_table(table)

        print(
            f"Created table {self._table_id}, "
            f"partitioned on column {table.time_partitioning.field}."
        )
        print('Sleeping for 1 seconds.')
        sleep(1)

    def insert_data(self, rows_to_insert: list[dict]) -> None:
        """
        Args:
            data: List[dict]
        """
        # Make an API request.
        errors = self._client.insert_rows_json(self._table_id, rows_to_insert)
        if not errors:
            print("New rows have been added.")
        else:
            print(f"Encountered errors while inserting rows: {errors}")

    def get_max_date(self) -> str:
        """Gets max date from stg_raw_questions table.

        Returns:
            str(date)
        """
        return_dt = ''
        query = (
            f"""
            SELECT
                DATE(last_modified_time) AS last_modified_time
            FROM 
                `{self._dataset_name}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE
                table_name = '{self._table_name}'
            """
        )
        rows = self._client.query_and_wait(query)  # Make an API request.
        for row in rows:
            # Row values can be accessed by field name or index.
            return_dt = row[0]
        return return_dt

# if __name__ == "__main__":
#     bq = BigQueryOperations(dataset_name="coding_questions", table_name="stg_raw_questions")
#     bq.get_max_date()
