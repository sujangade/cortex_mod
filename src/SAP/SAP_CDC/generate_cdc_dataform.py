# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
"""Generates Dataform CDC files from configuration."""

import argparse
import logging
import os
from pathlib import Path
import sys
import yaml
try:
    import jinja2
except ImportError:
    jinja2 = None

import base64

from google.cloud import bigquery
from googleapiclient import discovery

# Constants
_CONFIG_FILE = Path(__file__).parent.parent / "cdc_settings.yaml"
_GENERATED_DATAFORM_DIR = "generated_dataform"
_ANNOTATIONS_DIR = Path(__file__).parent.parent / "config/ecc/annotations/cdc"

# Initialize Client
client = None

def get_bq_client():
    global client
    if client is None:
        try:
            client = bigquery.Client()
        except Exception as e:
            logging.warning(f"Failed to initialize BigQuery Client: {e}")
            return None
    return client

def write_file_to_dataform(project, region, repository, workspace, file_path, content):
    """Writes a file to Dataform Workspace using Discovery API."""
    service = discovery.build('dataform', 'v1beta1')
    parent = f"projects/{project}/locations/{region}/repositories/{repository}/workspaces/{workspace}"
    
    # Content must be base64 encoded
    encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    
    body = {
        "contents": encoded_content,
        "path": file_path
    }
    
    try:
        service.projects().locations().repositories().workspaces().writeFile(
            name=f"{parent}/writeFile",
            body=body
        ).execute()
        logging.info(f"Successfully wrote {file_path} to Dataform.")
    except Exception as e:
        logging.error(f"Failed to write {file_path} to Dataform: {e}")


def get_primary_keys(full_table_name):
    """Retrieves primary key columns for raw table from metadata table.
    
    Start with logic from generate_query.py.
    """
    bq_client = get_bq_client()
    if not bq_client:
        logging.warning("No BQ Client available. Returning mock keys for testing.")
        return ["MOCK_KEY_1", "MOCK_KEY_2"]

    try:
        _, dataset, table_name = full_table_name.split('.')
    except ValueError:
        # Handle cases where project might be missing or different format
        parts = full_table_name.split('.')
        table_name = parts[-1]
        dataset = parts[-2]

    # Custom tables and fields in SAP have the prefix "/NAMESPACE/",
    # which are renamed to "NAMESPACE_" (or other character, per SLT settings)
    # when replicated to BigQuery, but retain their original naming in DD03L
    # table. So we need to mirror the replacement logic in the query to
    # get records for such tables.
    
    replace_char = '_'
    
    sap_naming_replace_logic = (
        'REPLACE('
        '  IF(SUBSTR({FIELD}, 1, 1) = "/", SUBSTR({FIELD}, 2), {FIELD}),'
        '  "/",'
        f'  "{replace_char}"'
        ')')
    bq_field_name = sap_naming_replace_logic.format(FIELD='fieldname')
    bq_table_name = sap_naming_replace_logic.format(FIELD='tabname')
    query = (f'SELECT {bq_field_name} as fieldname '
             f'FROM `{dataset}.dd03l` '
             f'WHERE KEYFLAG = "X" AND fieldname != ".INCLUDE" '
             f'AND {bq_table_name} = "{table_name.upper()}"')
    
    try:
        query_job = bq_client.query(query)
        fields = []
        for row in query_job:
            fields.append(row['fieldname'])
        return fields
    except Exception as e:
        logging.warning(f"Could not fetch primary keys for {full_table_name}: {e}")
        return []

def get_table_description(table_name):
    """Reads table and field descriptions from annotation yaml."""
    yaml_path = Path(__file__).parent.parent / f"config/ecc/annotations/cdc/{table_name}.yaml"
    if not yaml_path.exists():
        logging.warning(f"Annotation file not found: {yaml_path}")
        return None, {}

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            table_desc = data.get('description', '')
            fields = data.get('fields', [])
            field_descs = {f['name']: f.get('description', '') for f in fields}
            return table_desc, field_descs
    except Exception as e:
        logging.warning(f"Error reading annotation file {yaml_path}: {e}")
        return None, {}

def generate_dataform_file(table_config, source_dataset, target_schema_name, test_mode=False, dataform_config=None):
    """Generates a Dataform .sqlx file for the given table configuration."""
    base_table = table_config.get('base_table')
    if not base_table:
        logging.error("Missing base_table in config")
        return

    # If test mode and this is NOT the table we want, skip.
    if test_mode and test_mode != base_table:
        return

    logging.info(f"Generating Dataform for {base_table}...")

    # 1. Get Primary Keys
    full_table_name = f"{source_dataset}.{base_table}" # Assuming DD03L is in source dataset
    primary_keys = get_primary_keys(full_table_name)
    
    # 2. Get Descriptions
    table_desc, field_descs = get_table_description(base_table)

    # 3. Partitioning
    partition_by_col = "last_update_ts"

    # 4. Construct SQLX Content
    
    # Config Block
    config_block = "config {\n"
    config_block += '  type: "incremental",\n'
    config_block += f'  schema: "{target_schema_name}",\n'
    config_block += f'  name: "{base_table}",\n' 
    
    pk_str = ", ".join([f'"{pk}"' for pk in primary_keys])
    config_block += f'  uniqueKey: [{pk_str}],\n'
    
    config_block += '  bigquery: {\n'
    config_block += f'    partitionBy: "{partition_by_col}"\n'
    config_block += '  },\n'
    
    if table_desc:
        safe_desc = table_desc.replace('"', '\\"')
        config_block += f'  description: "{safe_desc}",\n'
    
    config_block += '  columns: {\n'
    # We technically need to list all columns to add descriptions? or just the ones we have?
    # The requirement example only showed a few. Let's add known ones.
    for col, desc in field_descs.items():
        if desc:
             safe_field_desc = desc.replace('"', '\\"')
             config_block += f'    {col}: "{safe_field_desc}",\n'
    
    # Always add op_flag description?
    config_block += '    op_flag: "Latest operation flag (I, U, D)"\n'
    config_block += '  }\n'
    config_block += '}\n\n'
    
    # 5. SQL Logic
    # We need to replace placeholders in standard template or just construct it here.
    # The template is logically simple enough to construct inline for now, 
    # but let's be careful with escaping.
    
    sql_logic = f"""-- STEP 1: Deduplicate the incoming SCD2 logs
-- We get the absolute latest state for every ID in the current batch
WITH incoming_data AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY {", ".join(primary_keys)}
      ORDER BY record_timestamp DESC
    ) as rank
  FROM
    ${{ref("stg_{base_table}_scd2")}}
  WHERE
    1 = 1
    -- Only process new records since the last run
    ${{when(incremental(), AND record_timestamp > (SELECT max(last_update_ts) FROM ${{self()}}) )}}
),

-- STEP 2: Filter for the latest record per ID
latest_state AS (
  SELECT
    * EXCEPT(rank),
    CURRENT_TIMESTAMP() as last_update_ts
  FROM
    incoming_data
  WHERE
    rank = 1
)

-- STEP 3: Merge into the Digital Twin
-- We include "D" records here so they "match" existing records and update the flag
SELECT
  *
FROM
  latest_state

---
-- STEP 4: Physical Cleanup
-- After the Merge (Insert/Update) is finished, we delete anything flagged 'D'
post_operations {{
  DELETE FROM ${{self()}} WHERE op_flag = 'D';
}}
"""
    
    full_content = config_block + sql_logic
    
    if test_mode:
        print(f"--- [TEST MODE] Generated Content for {base_table} ---")
        print(full_content)
        print("-----------------------------------------------------")
        return

    # Check config for Dataform API
    if dataform_config and dataform_config.get('repository') and dataform_config.get('workspace'):
        file_path = f"definitions/{base_table}.sqlx" # Assuming definitions/ root
        write_file_to_dataform(
            dataform_config['project'],
            dataform_config['region'],
            dataform_config['repository'],
            dataform_config['workspace'],
            file_path,
            full_content
        )
    else:
        # Local file write
        output_file = Path(_GENERATED_DATAFORM_DIR) / f"{base_table}.sqlx"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_content)
        logging.info(f"Generated locally: {output_file}")


def main():
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Generate Dataform CDC files.")
    parser.add_argument("--test-table", help="Test Mode: Print SQLX for a single table to stdout without writing files.", default=None)
    
    args = parser.parse_args()

    # Read Config
    try:
        import config_dataform as config
    except ImportError:
        logging.error("Could not import config_dataform.py. Please ensure it exists in the same directory.")
        sys.exit(1)

    source_project = config.SOURCE_PROJECT
    source_dataset = config.SOURCE_DATASET
    target_dataset = config.TARGET_DATASET
    sql_flavour = config.SQL_FLAVOUR
    
    # Dataform Config
    dataform_config = {
        'project': getattr(config, 'DATAFORM_PROJECT', None) or source_project,
        'region': getattr(config, 'DATAFORM_REGION', None),
        'repository': getattr(config, 'DATAFORM_REPOSITORY', None),
        'workspace': getattr(config, 'DATAFORM_WORKSPACE', None)
    }

    logging.info(f"Using Config: Project={source_project}, Source={source_dataset}, Target={target_dataset}, Flavour={sql_flavour}")
    
    if args.test_table:
        logging.info(f"Test Mode Enabled for table: {args.test_table}")
    elif dataform_config['repository'] and dataform_config['workspace']:
        logging.info(f"Dataform API Mode Enabled: Writing to {dataform_config['repository']}/{dataform_config['workspace']}")
    
    # Create Output Dir (only if not using API or if we want local backup? Let's keep it for now)
    os.makedirs(_GENERATED_DATAFORM_DIR, exist_ok=True)
    
    # Read Settings
    with open(_CONFIG_FILE, encoding="utf-8") as settings_file:
        settings_content = settings_file.read()
        
    if jinja2:
        t = jinja2.Template(settings_content,
                            trim_blocks=True,
                            lstrip_blocks=True)
        resolved_configs = t.render({"sql_flavour": sql_flavour})
    else:
        # Fallback for simple replacement if jinja2 is missing
        # Handle simple {% if sql_flavour == '...' %} blocks
        import re
        
        def render_template(content, context):
            # 1. Handle simple variable substitution {{ var }}
            for key, value in context.items():
                content = content.replace(f"{{{{ {key} }}}}", value).replace(f"{{{{{key}}}}}", value)
            
            # 2. Handle simple if blocks (very limited support)
            # Support: {% if sql_flavour.upper() == 'S4' %} ... {% endif %}
            
            # Regex to find if blocks
            if_pattern = re.compile(r'{%\s*if\s+(.*?)\s*%}(.*?){%\s*endif\s*%}', re.DOTALL)
            
            def eval_condition(match):
                condition = match.group(1).strip()
                inner_content = match.group(2)
                
                # Very basic eval for sql_flavour
                # Expected condition: sql_flavour.upper() == 'S4'
                
                # Replace variables in condition
                for key, value in context.items():
                    condition = condition.replace(key, f"'{value}'")
                
                # Evaluate
                try:
                    # simplistic evaluation: 'ECC'.upper() == 'S4'
                    # We might need to handle .upper()
                    # Let's just manually check the specific known condition
                    is_s4 = context.get('sql_flavour', '').upper() == 'S4'
                    
                    if "sql_flavour.upper() == 'S4'" in match.group(1):
                        return inner_content if is_s4 else ""
                    elif "sql_flavour == 'S4'" in match.group(1):
                        return inner_content if is_s4 else ""
                    
                    # If we can't parse it, safe fallback? or empty?
                    return "" 
                except:
                    return ""

            content = if_pattern.sub(eval_condition, content)
            return content

        resolved_configs = render_template(settings_content, {"sql_flavour": sql_flavour})
        
    try:
        configs = yaml.load(resolved_configs, Loader=yaml.SafeLoader)
    except Exception as e:
        logging.error(f"Error reading {_CONFIG_FILE}: {e}")
        sys.exit(1)
        
    table_configs = configs.get("data_to_replicate", [])
    
    # Process Tables
    for table_config in table_configs:
        # We need to pass the FULL dataset name for BQ queries if it's just a dataset name provided
        # args.source_dataset might be just "dataset" or "project.dataset"
        # Let's ensure we have project.dataset for the query
        
        full_source_dataset = source_dataset
        if "." not in full_source_dataset:
            full_source_dataset = f"{source_project}.{full_source_dataset}"
            
        generate_dataform_file(table_config, full_source_dataset, target_dataset, test_mode=args.test_table, dataform_config=dataform_config)

    if not args.test_table:
        logging.info("Done generating Dataform files.")

if __name__ == "__main__":
    main()
