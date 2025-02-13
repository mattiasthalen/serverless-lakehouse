import os
import yaml

# Read the schema file
with open('./pipelines/schemas/export/adventure_works.schema.yaml', 'r') as schema_file:
   schema = yaml.safe_load(schema_file)

# Get tables from schema
tables = [table_name for table_name in schema['tables'].keys() 
         if table_name.startswith("raw__")]

print("Found tables:", tables)

# Create SQL files for each table
for table in tables:
   model_name = table.replace("raw__", "stg__")
   file_path = f"./models/silver/{model_name}.sql"
   
   # Get columns and find primary key
   table_schema = schema['tables'][table]
   columns = {col_name: col_info 
             for col_name, col_info in table_schema.get('columns', {}).items()
             if not col_name.startswith('_dlt')}
   
   # Get single primary key with PK_NOT_FOUND as default
   primary_key = next((col_name for col_name, col_info in columns.items() 
                      if col_info.get('primary_key')), 'PK_NOT_FOUND')
   
   # Build column list
   formatted_columns = ',\n   '.join(columns.keys())
   
   body = f"""MODEL (
  kind VIEW
);

SELECT
   {formatted_columns},
   TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at,
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
   CASE
       WHEN _sqlmesh__is_current
       THEN _sqlmesh__loaded_at
       ELSE _sqlmesh__valid_to
   END AS _sqlmesh__updated_at
FROM delta_scan("./lakehouse/bronze/{table}")"""
   
   # Ensure the models/silver directory exists
   os.makedirs(os.path.dirname(file_path), exist_ok=True)
   
   with open(file_path, 'w') as file:
       file.write(body)

print(f"Created {len(tables)} SQL files in ./models/silver/")
print(f"Created external_models.yaml with {len(tables)} table definitions")