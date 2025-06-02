# VNP Pip Package

## Overview

`vnp-pip-package` (or `vnp` when imported in Python) is a utility library designed to provide common functionalities and standardized components for VNP's Python projects. It aims to reduce code duplication and promote best practices across different projects.

## Modules

The package currently includes the following modules:

- **bigquery.py**:

  - Provides the `BigQueryController` class for simplified interaction with Google BigQuery.
  - Supports creating tables, checking table existence, loading data from JSON, loading data from Google Cloud Storage (GCS), and executing queries.
  - Offers methods for schema inference from JSON data and efficient data loading via GCS staging.

- **logger.py**:

  - Provides the `get_logger` function for obtaining configured logger instances.
  - Standardizes logging format and setup across projects.
  - Configures both root logger and common third-party library loggers to ensure consistent logging behavior.

- **secret_manager.py**

  - Provides the `get_secret` function for retrieving secrets from Google Secret Manager.
  - Enables to get, create, delete and lists the secrets.

- **storage.py**:
  - Provides the `StorageManager` class for managing Google Cloud Storage operations.
  - Supports uploading data (dictionaries, lists of dictionaries, Pandas DataFrames) to GCS as JSON files.
  - Includes functionality for deleting files from GCS.

## Building and Publishing

To build and publish the package, follow these steps:

1. Install build dependencies:

   ```bash
   uv add build twine
   ```

2. Build the package:

   ```bash
   uv build
   ```

3. Publish the package:

   ```bash
   uv run twine upload --verbose dist/*
   ```

## Usage

After installation, you can import and use the modules and classes provided by `vnp_utils` in your Python projects.

**Example (using BigQueryController):**

```python
from vnp_utils.bigquery import BigQueryController

# ... (Assume bigquery_payload and key_path are defined) ...

bq_controller = BigQueryController(bigquery_payload=bigquery_payload, key_path=key_path)

# Example: Export JSON data to BigQuery via GCS
gcs_uri = bq_controller.export_to_bq_via_gcs(json_data=data)
print(f"Data loaded to BigQuery via GCS: {gcs_uri}")
```

Refer to the individual module files (`bigquery.py`, `logger.py`, `storage.py`) for detailed class and function documentation and usage examples.
