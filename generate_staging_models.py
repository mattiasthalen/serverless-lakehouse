import os

# Scan the bronze folder for raw__ subfolders
bronze_path = "./lakehouse/bronze"
tables = [folder for folder in os.listdir(bronze_path) 
          if os.path.isdir(os.path.join(bronze_path, folder)) 
          and folder.startswith("raw__")]

print("Found tables:", tables)  # Print for verification

# Create SQL files for each table
for table in tables:
    model_name = table.replace('raw__', 'stg__')
    file_path = f"./models/silver/{model_name}.sql"
    
    # Ensure the models/bronze directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    body = f"""MODEL (
    kind VIEW
);

@DEF(primary_key, ENTER_THE_PRIMARY_KEY);

WITH cte__source AS (
    SELECT
        *
    FROM delta_scan("{bronze_path}/{table}")
), cte__validity AS(
    SELECT
        *,
        TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _sqlmesh__loaded_at,
        ROW_NUMBER() OVER (PARTITION BY @primary_key ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
        CASE
            WHEN _sqlmesh__version = 1
            THEN '1970-01-01 00:00:00'::TIMESTAMP
            ELSE _sqlmesh__loaded_at
        END AS _sqlmesh__valid_from,
        COALESCE(
            LEAD(_sqlmesh__loaded_at) OVER (PARTITION BY @primary_key ORDER BY _sqlmesh__loaded_at),
            '9999-12-31 23:59:59'::TIMESTAMP
        ) AS _sqlmesh__valid_to,
        _sqlmesh__valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__is_current,
        CASE
            WHEN _sqlmesh__is_current
            THEN _sqlmesh__loaded_at
            ELSE _sqlmesh__valid_to
        END AS _sqlmesh__updated_at
    
    FROM cte__source
)
SELECT * FROM cte__validity
"""
    
    with open(file_path, 'w') as file:
        file.write(body)

print(f"Created {len(tables)} SQL files in ./models/bronze/")