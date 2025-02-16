import os
import re

def generate_uss_bridges():
    # Define path
    source_dir = "./models/silver"
    
    # Regular expressions to capture _pit_hook__ and _hook__ column names
    pit_hook_pattern = re.compile(r"(_pit_hook__\w+)")
    hook_pattern = re.compile(r"(_hook__\w+)")
    meta_pattern = re.compile(r"(\w+__(?:record_loaded_at|record_valid_from|record_valid_to|record_updated_at))")
    
    filenames = [filename for filename in os.listdir(source_dir) if filename.startswith("bag__") and filename.endswith(".sql")]
    
    # Process each SQL file in the source directory
    for filename in filenames:
        source_path = os.path.join(source_dir, filename)
        target_filename = filename.replace("bag__", "uss__bridge__")
        target_path = os.path.join(source_dir, target_filename)

        with open(source_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Extract fields
        pit_hooks = sorted(set(pit_hook_pattern.findall(content)))
        hooks = sorted(set(hook_pattern.findall(content)))
        
        meta = sorted(set(meta_pattern.findall(content)))
        meta_renamed = [f"{field} AS bridge__{field.split('__')[1]}" for field in meta]

        if not (pit_hooks or hooks):
            print(f"Skipping {filename} (no hooks found)")
            continue

        # Extract base & stage name 
        base_name = filename.replace(".sql", "")
        stage_name = base_name.split("__")[2]

        # Generate SQL content
        sql_content = f"""MODEL (
  kind VIEW
);

WITH bridge AS (
  SELECT
    '{stage_name}' AS stage,
    {',\n    '.join(pit_hooks + hooks + meta_renamed)},
    bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
  FROM silver.{base_name}
)
SELECT * FROM bridge
"""

        # Write to new file
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
    
    print(f"Generated {len(filenames)} uss bridge in ./models/silver/")
    
if __name__ == "__main__":
    generate_uss_bridges()