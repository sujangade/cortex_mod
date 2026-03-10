# Configuration for generate_cdc_dataform.py

# Google Cloud Project ID where the source data resides
SOURCE_PROJECT = "YOUR_SOURCE_PROJECT"

# BigQuery Dataset Name where the raw SAP data is replicated (e.g., via SLT)
# If the dataset is in the same project as SOURCE_PROJECT, just provide the dataset name.
# Otherwise, provide "project.dataset".
SOURCE_DATASET = "YOUR_SOURCE_DATASET"

# BigQuery Dataset Name where the Dataform models will be created/targeted
# This is used for the `schema` config in the generated Dataform files.
TARGET_DATASET = "YOUR_TARGET_DATASET"

# SQL Flavour for the source system (ECC or S4)
# This mimics the behavior of the original Cortex scripts to load the correct settings.
SQL_FLAVOUR = "ECC"

# Dataform Configuration
# Leave these empty if you only want to generate files locally or test.
DATAFORM_PROJECT = "YOUR_DATAFORM_PROJECT" # Defaults to SOURCE_PROJECT if not set
DATAFORM_REGION = "us-central1"
DATAFORM_REPOSITORY = "YOUR_REPOSITORY_NAME"
DATAFORM_WORKSPACE = "YOUR_WORKSPACE_NAME"

