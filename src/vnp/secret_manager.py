"""
Secret Manager Utilities.

This module provides a class for interacting with Google Cloud Secret Manager.
"""

from __future__ import annotations

from google.cloud import secretmanager

from vnp.logger import get_logger

logger = get_logger(__name__)


class SecretManager:
    """
    A class to interact with Google Cloud Secret Manager.

    Attributes:
        client (secretmanager.SecretManagerServiceClient): The client for interacting with Secret Manager.
        project_id (str): The ID of the Google Cloud project.

    """

    def __init__(self, project_id: str) -> None:
        """
        Initialize the GCPSecretsManager client.

        Args:
            project_id: The ID of your Google Cloud project.

        """
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id
        self.parent = f"projects/{project_id}"

    def get_secret(self, secret_id: str, version: str = "latest") -> bytes:
        """
        Retrieve the value of a secret.

        Args:
            secret_id: The ID of the secret to retrieve.
            version: The version of the secret to retrieve (default: "latest").

        Returns:
            The secret data as bytes.

        """
        logger.info(f"{'=' * 10} Retrieving secret from GCP. {'=' * 10}")
        name = f"{self.parent}/secrets/{secret_id}/versions/{version}"
        response = self.client.access_secret_version(name=name)
        return response.payload.data

    def add_secret(
        self, secret_id: str, secret_data: bytes, labels: dict | None = None
    ) -> secretmanager.Secret:
        """
        Add a new secret or a new version to an existing secret.

        Args:
            secret_id: The ID of the secret to create or update.
            secret_data: The secret data as bytes.
            labels: An optional dictionary of labels to associate with the secret.

        Returns:
            The created or updated Secret object.

        Raises:
            Exception: If an unexpected error occurs during secret creation or version addition.

        """
        logger.info(f"{'=' * 10} Adding secret to GCP {'=' * 10}")
        secret_name = f"{self.parent}/secrets/{secret_id}"
        try:
            # Try to get the secret to check if it already exists
            self.client.get_secret(name=secret_name)
            logger.info(f"Secret {secret_id} already exists, adding a new version")
            # Secret exists, so add a new version
            payload = secretmanager.SecretPayload(data=secret_data)
            response = self.client.add_secret_version(
                parent=secret_name, payload=payload
            )
            return self.client.get_secret(
                name=secret_name
            )  # Return the updated secret object

        except Exception as e:
            if "not found" in str(e).lower():
                logger.info(f"Secret {secret_id} not found, creating a new one")
                # Secret does not exist, create a new one
                secret = secretmanager.Secret(
                    replication=secretmanager.Replication(
                        automatic=secretmanager.Replication.Automatic()
                    ),
                    labels=labels if labels else {},
                )
                response = self.client.create_secret(
                    parent=self.parent, secret_id=secret_id, secret=secret
                )
                payload = secretmanager.SecretPayload(data=secret_data)
                self.client.add_secret_version(parent=secret_name, payload=payload)
                return response
            logger.exception(f"Error adding/updating secret {secret_id}.")
            raise

    def delete_secret(self, secret_id: str) -> None:
        """
        Delete secret and all of its versions.

        Args:
            secret_id: The ID of the secret to delete.

        """
        logger.warning(f"{'=' * 10} Deleting secret from GCP {'=' * 10}")
        name = f"{self.parent}/secrets/{secret_id}"
        self.client.delete_secret(name=name)

    def list_secrets(self) -> list[secretmanager.Secret]:
        """
        List all secrets in the project.

        Returns:
            A list of Secret objects.

        """
        logger.info(f"{'=' * 10} Listing secrets from GCP {'=' * 10}")
        return self.client.list_secrets(parent=self.parent)
