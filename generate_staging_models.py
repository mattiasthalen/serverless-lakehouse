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

WITH cte__source AS (
    SELECT
        *
    FROM delta_scan("{bronze_path}/{table}")
)
SELECT * FROM cte__source
"""
    
    with open(file_path, 'w') as file:
        file.write(body)

print(f"Created {len(tables)} SQL files in ./models/bronze/")