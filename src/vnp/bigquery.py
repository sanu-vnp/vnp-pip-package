"""
BigQuery controller module.

This module provides a class for interacting with BigQuery and loading JSON data
via Cloud Storage staging with schema autodetection.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar, cast
from zoneinfo import ZoneInfo

from pydantic import BaseModel

# Only import Callable if type checking is active
if TYPE_CHECKING:
    from collections.abc import Callable

from google.api_core.exceptions import GoogleAPIError, ServerError, ServiceUnavailable
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from vnp.logger import get_logger
from vnp.storage import StorageController

logger = get_logger(__name__)

# Type variable for generic return
T = TypeVar("T")


# Structure of bigquery_payload using dataclass
class BigqueryPayload(BaseModel):
    """
    Configuration payload for BigQuery operations.

    Attributes:
        project_id (str): GCP project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        bucket_name (str | None): GCS bucket name for data staging

    """

    project_id: str
    dataset_id: str
    table_id: str
    bucket_name: str | None = None


# Default configuration
# Explicitly type the DEFAULT_CONFIG dictionary
DEFAULT_CONFIG: dict[str, int | float | str] = {
    "max_retries": 3,
    "initial_retry_delay": 1,  # seconds
    "retry_multiplier": 2,
    "max_retry_delay": 60,  # seconds
    "chunk_size": 10000,  # records per chunk
    "temp_table_prefix": "temp_",
    "temp_table_suffix_format": "%Y%m%d%H%M%S",
}


def retry_on_transient_error(
    max_retries: int = 3,
    initial_delay: float = 1,
    backoff_factor: float = 2,
    max_delay: float = 60,
) -> Callable[..., Any]:
    """
    Retries a function on transient errors with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for the delay after each retry
        max_delay: Maximum delay between retries in seconds

    Returns:
        Callable: The decorated function

    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (
                    ServerError,
                    ServiceUnavailable,
                    ConnectionError,
                    TimeoutError,
                ) as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = min(delay, max_delay)
                        warning_msg = (
                            f"{'=' * 10} Transient error occurred: {e!s}. "
                            f"Retrying in {sleep_time:.1f} seconds. "
                            f"(Attempt {attempt + 1}/{max_retries}) {'=' * 10}"
                        )
                        logger.warning(warning_msg)
                        time.sleep(sleep_time)
                        delay *= backoff_factor
                    else:
                        error_msg = f"{'=' * 10} Operation failed after {max_retries} retries {'=' * 10}"
                        logger.exception(error_msg)
                        raise
                except Exception as e:
                    error_msg = (
                        f"{'=' * 10} Non-transient error occurred: {e!s} {'=' * 10}"
                    )
                    logger.exception(error_msg)
                    raise

            # Handle unexpected loop exit
            unexpected_error_msg = (
                "Retry loop exited unexpectedly without catching an exception"
            )
            if last_exception:
                raise last_exception
            raise RuntimeError(unexpected_error_msg)

        return wrapper

    return decorator


class BigQueryController:
    """
    BigQuery controller class with Cloud Storage staging support and schema autodetection.

    Attributes:
        dataset_id (str): BigQuery dataset ID
        project_id (str): GCP project ID
        table_id (str): BigQuery table ID
        bucket_name (str): GCS bucket name for data staging
        client (bigquery.Client): BigQuery client instance
        storage_controller (StorageController): Storage controller for GCS operations
        config (dict): Controller configuration options

    """

    def __init__(
        self, bigquery_payload: BigqueryPayload, config: dict[str, Any] | None = None
    ) -> None:
        """
        Initialize the BigQuery client with service account credentials.

        Args:
            bigquery_payload: Configuration payload with project, dataset, table IDs and bucket name
            config: Optional configuration overrides

        Raises:
            Exception: If initialization fails
            ValueError: If bucket_name is not provided

        """
        try:
            self.dataset_id = bigquery_payload.dataset_id
            self.project_id = bigquery_payload.project_id
            self.table_id = bigquery_payload.table_id
            self.bucket_name = bigquery_payload.bucket_name

            # Merge provided config with defaults
            # Ensure self.config has the correct type hint
            self.config: dict[str, int | float | str] = {**DEFAULT_CONFIG}
            if config:
                self.config.update(config)

            # Initialize clients using automatically detected credentials
            self.client = bigquery.Client(project=self.project_id)
            self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

            # Ensure bucket_name is not None before passing to StorageController
            if self.bucket_name is None:
                error_message = "Bucket name must be provided in bigquery_payload"
                raise ValueError(error_message)

            self.storage_controller = StorageController(
                project_id=self.project_id,
                bucket_name=self.bucket_name,
                credentials=self.client._credentials,
            )

            logger.info(
                f"{'=' * 10} BigQueryController initialized for {self.dataset_id}.{self.table_id} {'=' * 10}"
            )
        except Exception:
            logger.exception(
                f"{'=' * 10} Failed to initialize BigQueryController. {'=' * 10}"
            )
            raise

    @retry_on_transient_error()
    def _load_data_from_gcs(self, gcs_uri: str, write_disposition: str) -> None:
        """
        Load data from GCS without schema evolution.

        Args:
            gcs_uri: GCS URI of the data file
            write_disposition: BigQuery write disposition

        Raises:
            Exception: If loading fails

        """
        try:
            # Get current table schema
            table = self.client.get_table(self.table_ref)

            # Load data with the current schema
            job_config = bigquery.LoadJobConfig(
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition=write_disposition,
                schema=table.schema,
            )
            load_job = self.client.load_table_from_uri(
                gcs_uri, self.table_ref, job_config=job_config
            )
            load_job.result()

        except Exception:
            logger.exception(f"{'=' * 10} Failed to load data {'=' * 10}")
            raise

    @retry_on_transient_error()
    def _create_partitioned_table(self, temp_suffix: str | None = None) -> None:
        """
        Create a time-partitioned table using the detected schema.

        Creates a partitioned table on import_timestamp field without partition expiration.
        Partitioned filter is not required.

        Args:
            temp_suffix: Optional suffix for the temporary table name

        Raises:
            Exception: For other unexpected errors during the operation

        """
        logger.debug(f"{'=' * 10} Creating partition on the table {'=' * 10}")

        try:
            # Get current table schema
            table = self.client.get_table(self.table_ref)
            schema = list(table.schema)

            # Handle import_timestamp field
            import_timestamp_field = next(
                (field for field in schema if field.name == "import_timestamp"), None
            )

            if not import_timestamp_field:
                schema.append(
                    bigquery.SchemaField(
                        name="import_timestamp", field_type="TIMESTAMP", mode="NULLABLE"
                    )
                )
            elif import_timestamp_field.field_type != "TIMESTAMP":
                schema = [field for field in schema if field.name != "import_timestamp"]
                schema.append(
                    bigquery.SchemaField(
                        name="import_timestamp", field_type="TIMESTAMP", mode="NULLABLE"
                    )
                )

            # Generate temporary table name
            temp_suffix_format_str = cast(
                str, self.config["temp_table_suffix_format"]
            )  # Type assertion
            temp_suffix = temp_suffix or datetime.now(
                ZoneInfo("Europe/Berlin")
            ).strftime(
                temp_suffix_format_str,
            )
            temp_table_prefix_str = cast(
                str, self.config["temp_table_prefix"]
            )  # Type assertion
            temp_table_id = f"{self.table_id}_{temp_table_prefix_str}{temp_suffix}"
            temp_table_ref = self.client.dataset(self.dataset_id).table(temp_table_id)

            # Create new partitioned table with the temporary name
            new_table = bigquery.Table(temp_table_ref, schema=schema)
            new_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="import_timestamp",
                require_partition_filter=False,  # Partition filter not required
                expiration_ms=None,  # No expiration
            )

            # Create the temporary table
            created_table = self.client.create_table(new_table)
            logger.debug(
                f"{'=' * 10} Created temporary partitioned table {created_table.full_table_id} {'=' * 10}"
            )

            # Copy data from original table to the temporary table
            job_config = bigquery.QueryJobConfig()
            job_config.destination = temp_table_ref
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

            query = f"""
            SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            """

            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            # Delete the original table and rename the temporary one
            self.client.delete_table(self.table_ref)
            self.client.get_table(
                temp_table_ref
            )  # Ensure the temp table exists before renaming

            # Use UPDATE DDL to rename the table
            rename_query = f"""
            ALTER TABLE `{self.project_id}.{self.dataset_id}.{temp_table_id}`
            RENAME TO `{self.table_id}`
            """

            rename_job = self.client.query(rename_query)
            rename_job.result()

            logger.info(
                f"{'=' * 10} Created partitioned table {self.project_id}.{self.dataset_id}.{self.table_id} {'=' * 10}",
            )
        except Exception:
            logger.exception(
                f"{'=' * 10} Failed to create partitioned table. {'=' * 10}"
            )
            raise

    @retry_on_transient_error()
    def _create_new_table_with_chunk(
        self, gcs_uri: str, write_disposition: str
    ) -> None:
        """
        Create a new table with schema autodetection using the first data chunk.

        Args:
            gcs_uri: GCS URI of the data file
            write_disposition: BigQuery write disposition

        """
        logger.info(
            f"{'=' * 10} Table {self.table_id} does not exist. Using schema autodetection. {'=' * 10}"
        )

        # Initial load with autodetection
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition=write_disposition,
        )
        load_job = self.client.load_table_from_uri(
            gcs_uri, self.table_ref, job_config=job_config
        )
        load_job.result()

        # Set up partitioning
        self._create_partitioned_table()

        # Reload data into partitioned table
        job_config = bigquery.LoadJobConfig(
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition=write_disposition,
            schema=self.client.get_table(self.table_ref).schema,
        )
        load_job = self.client.load_table_from_uri(
            gcs_uri, self.table_ref, job_config=job_config
        )
        load_job.result()

    @retry_on_transient_error()
    def _table_exists(self) -> bool:
        """
        Check if a table exists in the dataset.

        Returns:
            bool: True if the table exists, False otherwise

        Raises:
            GoogleAPIError: On non-NotFound BigQuery API errors

        """
        try:
            self.client.get_table(self.table_ref)
        except NotFound:
            return False
        except GoogleAPIError:
            logger.exception(f"{'=' * 10} Error checking if table exists. {'=' * 10}")
            raise
        else:
            return True

    def _chunk_data(
        self, data_list: list[dict[str, Any]]
    ) -> list[list[dict[str, Any]]]:
        """
        Split a large list of records into smaller chunks.

        Args:
            data_list: list of data records

        Returns:
            list[list[dict[str, Any]]]: list of data chunks

        """
        # Use cast to explicitly tell MyPy that chunk_size is an int here
        chunk_size = cast(int, self.config["chunk_size"])
        return [
            data_list[i : i + chunk_size] for i in range(0, len(data_list), chunk_size)
        ]

    @staticmethod
    def _add_import_timestamp(json_data: dict | list[dict]) -> dict | list[dict]:
        """
        Add import_timestamp field to JSON data in ISO format.

        Args:
            json_data: Original JSON data (dict or list of dicts)

        Returns:
            Union[dict, list[dict]]: JSON data with import_timestamp added

        Raises:
            TypeError: If json_data is not a dict or list of dicts

        """
        if not isinstance(json_data, (dict, list)):
            error_message = f"Expected dict or list, got {type(json_data).__name__}"
            raise TypeError(error_message)

        import_timestamp = datetime.now(UTC).isoformat()

        if isinstance(json_data, dict):
            json_data["import_timestamp"] = import_timestamp
            return json_data

        if isinstance(json_data, list):
            if not all(isinstance(item, dict) for item in json_data):
                error_message = "All items in list must be dictionaries"
                raise TypeError(error_message)

            for item in json_data:
                item["import_timestamp"] = import_timestamp

        return json_data

    @retry_on_transient_error()
    def export_to_bigquery(
        self,
        json_data: dict[str, Any] | list[dict[str, Any]],
        write_disposition: str = "WRITE_APPEND",
        delete_gcs_file: bool = True,
    ) -> list[str]:
        """
        Export JSON data to BigQuery via Cloud Storage.

        Features:
        - For new tables: Uses schema autodetection
        - For existing tables: Uses existing schema
        - Handles large datasets by chunking
        - Includes retry logic for transient errors

        Args:
            json_data: JSON data to load (dict or list of dicts)
            write_disposition: BigQuery write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
            delete_gcs_file: Whether to delete the staging file after loading

        Returns:
            list[str]: URIs of the files in GCS

        Raises:
            ValueError: For invalid input data
            Exception: For any general errors during the process

        """
        try:
            # Validate write disposition
            valid_dispositions = ["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"]
            if write_disposition not in valid_dispositions:
                error_message = f"Invalid write_disposition. Must be one of {', '.join(valid_dispositions)}"
                raise ValueError(error_message)

            # Add import_timestamp to the data
            json_data_with_timestamp = BigQueryController._add_import_timestamp(
                json_data
            )

            # Ensure we have a list
            data_list = (
                json_data_with_timestamp
                if isinstance(json_data_with_timestamp, list)
                else [json_data_with_timestamp]
            )
            # Return early if there's no data to process
            if not data_list:
                logger.info(
                    f"{'=' * 10} No data to process, skipping BigQuery export {'=' * 10}"
                )
                return []

            # Check if we need to chunk the data
            # Use cast to explicitly tell MyPy that chunk_size is an int here
            chunk_size = cast(int, self.config["chunk_size"])
            chunks = (
                self._chunk_data(data_list)
                if len(data_list) > chunk_size
                else [data_list]
            )
            logger.info(
                f"{'=' * 10} Processing {len(data_list)} records in {len(chunks)} chunks {'=' * 10}"
            )

            # Check if table exists
            table_existed = self._table_exists()

            # URIs of the uploaded files
            gcs_uris = []

            # Process each chunk
            for i, chunk in enumerate(chunks):
                # Skip empty chunks
                if not chunk:
                    logger.info(f"{'=' * 10} Skipping empty chunk {i} {'=' * 10}")
                    continue
                # Generate a unique identifier for the chunk file
                chunk_id = f"{uuid.uuid4().hex[:8]}_{i}"

                # Upload the chunk to GCS
                gcs_uri = self.storage_controller.upload_to_gcs(
                    chunk, prefix=f"{self.table_id}_data_{chunk_id}"
                )
                gcs_uris.append(gcs_uri)

                if not table_existed and i == 0:
                    # For the first chunk of a new table: use schema autodetection
                    self._create_new_table_with_chunk(gcs_uri, write_disposition)
                    table_existed = True  # Table now exists for subsequent chunks
                else:
                    # For existing tables or subsequent chunks: use current schema
                    self._load_data_from_gcs(gcs_uri, write_disposition)

                # Clean up staging file if requested
                if delete_gcs_file:
                    self.storage_controller.delete_file(gcs_uri)

            # If we processed any data, log results
            if gcs_uris:
                table = self.client.get_table(self.table_ref)
                logger.info(
                    f"{'=' * 10} Loaded {len(data_list)} records into {self.table_id} {'=' * 10}"
                )
                logger.info(
                    f"{'=' * 10} Table {self.table_id} now has {table.num_rows} rows {'=' * 10}"
                )

        except Exception:
            logger.exception(
                f"{'=' * 10} Failed to export data to BigQuery. {'=' * 10}"
            )
            raise

        else:
            return gcs_uris

    @retry_on_transient_error()
    def execute_query(self, query: str) -> list[dict]:
        """
        Execute a SQL query and return results as a list of dictionaries.

        Args:
            query: SQL query to execute

        Returns:
            list[dict]: Query results as a list of dictionaries

        Raises:
            Exception: If query execution fails

        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception:
            logger.exception(f"{'=' * 10} Query execution failed. {'=' * 10}")
            raise
