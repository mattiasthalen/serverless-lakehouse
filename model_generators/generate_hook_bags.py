import os
import yaml

# Read the schema file
with open('./pipelines/schemas/export/adventure_works.schema.yaml', 'r') as schema_file:
    schema = yaml.safe_load(schema_file)

# Get tables from schema
tables = [table_name for table_name in schema['tables'].keys() 
         if table_name.startswith("raw__")]

print("Found tables:", tables)

def get_hook_columns(columns):
    """Find columns that end with '_id' to generate hooks."""
    return [col_name for col_name in columns.keys() if col_name.endswith('_id')]

def sort_columns(columns, primary_key):
    """Sort columns into primary key, foreign keys, and other columns."""
    foreign_keys = [col for col in columns.keys() if col.endswith('_id') and col != primary_key]
    other_columns = [col for col in columns.keys() if col != primary_key and col not in foreign_keys]
    
    return [primary_key] + sorted(foreign_keys) + sorted(other_columns)

# Create SQL files for each table
for table in tables:
    model_name = table.replace("raw__", "bag__")
    file_path = f"./models/silver/{model_name}.sql"
    
    # Get columns and find primary key
    table_schema = schema['tables'][table]
    columns = {col_name: col_info 
              for col_name, col_info in table_schema.get('columns', {}).items()
              if not col_name.startswith('_dlt')}
    
    # Get single primary key with PK_NOT_FOUND as default
    primary_key = next((col_name for col_name, col_info in columns.items() 
                       if col_info.get('primary_key')), 'PK_NOT_FOUND')
    
    # Sort and format columns
    sorted_columns = sort_columns(columns, primary_key)
    formatted_columns = ',\n    '.join(sorted_columns)
    
    # Generate hooks for ID columns
    hook_columns = get_hook_columns(columns)
    hook_statements = []
    
    # Get entity name from primary key by removing '_id'
    entity_name = primary_key[:-3] if primary_key.endswith('_id') else primary_key
    
    # Add PIT hook for primary key
    pit_hook = f"    CONCAT('{entity_name}|adventure_works|', {primary_key}, '~epoch|valid_from|', _sqlmesh__valid_from)::BLOB AS _pit_hook__{entity_name}"
    hook_statements.append(pit_hook)
    
    # Add regular hook for primary key
    primary_hook = f"    CONCAT('{entity_name}|adventure_works|', {primary_key})::BLOB AS _hook__{entity_name}"
    hook_statements.append(primary_hook)
    
    # Add hooks for foreign keys
    for col in sorted(hook_columns):
        if col != primary_key:  # Skip primary key as it's already handled
            referenced_entity = col[:-3]  # Remove '_id' suffix
            hook = f"    CONCAT('{referenced_entity}|adventure_works|', {col})::BLOB AS _hook__{referenced_entity}"
            hook_statements.append(hook)
    
    hook_definitions = ',\n'.join(hook_statements)
    
    body = f"""MODEL (
  kind VIEW
);

WITH staging AS (
  SELECT
    {formatted_columns},
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at
  FROM DELTA_SCAN("./lakehouse/bronze/{table}")
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    CASE
      WHEN _sqlmesh__version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE _sqlmesh__loaded_at
    END AS _sqlmesh__valid_from,
    COALESCE(
      LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY {primary_key} ORDER BY _sqlmesh__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
    CASE WHEN _sqlmesh__is_current THEN _sqlmesh__loaded_at ELSE _sqlmesh__valid_to END AS _sqlmesh__updated_at
  FROM staging
), hooks AS (
  SELECT
{hook_definitions},
    *
  FROM validity
)
SELECT
  *
FROM hooks"""
    
    # Ensure the models/silver directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as file:
        file.write(body)

print(f"Generated {len(tables)} hook models in ./models/silver/")