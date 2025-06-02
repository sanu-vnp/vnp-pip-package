"""
Storage utilities for Cloud Storage operations.

This module provides a class for interacting with Google Cloud Storage.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
from google.oauth2.service_account import Credentials

from vnp.logger import get_logger

if TYPE_CHECKING:
    from google.oauth2.service_account import Credentials


class StorageController:
    """
    Google Cloud Storage manager for staging data.

    Attributes:
        project_id (str): Google Cloud project ID
        bucket_name (str): Cloud Storage bucket name
        credentials: Google service account credentials

    """

    def __init__(
        self, project_id: str, bucket_name: str, credentials: Credentials
    ) -> None:
        """
        Initialize the Cloud Storage client and get or create the bucket.

        Args:
            project_id: Google Cloud project ID
            bucket_name: Cloud Storage bucket name
            credentials: Google service account credentials

        """
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials = credentials
        self.logger = get_logger(__name__)

        # Initialize the Storage client
        self.storage_client = storage.Client(
            credentials=self.credentials,
            project=self.project_id,
        )

        # Get or create the bucket
        try:
            self.bucket = self.storage_client.get_bucket(bucket_name)
        except NotFound:
            self.bucket = self.storage_client.create_bucket(bucket_name)
            self.logger.info(f"Created new bucket: {bucket_name}")

    def upload_to_gcs(
        self, data: dict | list[dict] | pd.DataFrame, prefix: str = "data"
    ) -> str:
        """
        Upload data to Google Cloud Storage.

        Args:
            data (Union[dict, list[dict], pd.DataFrame]): Data to upload (dict, list of dicts, or pandas DataFrame).
            prefix (str): Prefix for the filename in GCS. Defaults to "data".

        Returns:
            str: URI of the uploaded file in GCS.

        Raises:
            Exception: If an error occurs during the upload process.

        """
        self.logger.debug(f"{'=' * 10} Exporting data to GCS {'=' * 10}")
        temp_file_path: Path | None = None
        # Generate a unique filename
        filename = f"{prefix}_{uuid.uuid4().hex}.json"

        # Convert data to a pandas DataFrame if it's not already
        if isinstance(data, (dict, list)):
            dataframe_data = (
                pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            )
        else:
            dataframe_data = data

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                delete=False,
                suffix=".json",
                encoding="utf-8",  # Added encoding
            ) as temp_file:
                dataframe_data.to_json(temp_file.name, orient="records", lines=True)
                temp_file_path = Path(temp_file.name)  # Changed to Path object

            # Upload the file to GCS
            blob = self.bucket.blob(filename)
            blob.upload_from_filename(temp_file_path)

            # Log success
            self.logger.info(
                f"Successfully uploaded data to gs://{self.bucket_name}/{filename}"
            )

        except Exception:
            self.logger.exception("Error uploading to GCS.")
            raise

        else:
            return f"gs://{self.bucket_name}/{filename}"

        finally:
            # Clean up the temporary file
            if temp_file_path and temp_file_path.exists():
                temp_file_path.unlink()

    def delete_file(self, gcs_uri: str) -> None:
        """
        Delete a file from Google Cloud Storage.

        Args:
            gcs_uri: URI of the file in GCS to delete

        """
        self.logger.debug(f"{'=' * 10} Deleting data from GCS {'=' * 10}")
        try:
            # Extract blob name from URI
            blob_name = gcs_uri.replace(f"gs://{self.bucket_name}/", "")

            # Delete the blob
            self.bucket.blob(blob_name).delete()
            self.logger.info(f"Deleted file: {gcs_uri}")

        except NotFound:
            # File not found is often acceptable for cleanup operations
            self.logger.warning(
                f"File not found during deletion: {gcs_uri}. It may have already been deleted."
            )
        except GoogleCloudError:
            # Catch other Google Cloud related errors
            self.logger.exception(f"Google Cloud error deleting file {gcs_uri}.")
        except Exception:
            # Catch any other unexpected exceptions.
            self.logger.exception(
                f"An unexpected error occurred while deleting file {gcs_uri}."
            )
