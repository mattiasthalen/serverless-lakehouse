import os
import re

def generate_uss_peripherals():
    # Define path to the source and target directories
    source_dir = "./models/silver"
    target_dir = "./models/gold"
    
    # Regular expressions to capture _pit_hook__ and _hook__ column names
    pit_hook_pattern = re.compile(r"(_pit_hook__\w+)")
    hook_pattern = re.compile(r"(_hook__\w+)")
    
    # Process each SQL file in the source directory
    filenames = [filename for filename in os.listdir(source_dir) if filename.startswith("uss_bridge__") and filename.endswith(".sql")]
    
    for filename in filenames:
        source_path = os.path.join(source_dir, filename)
        
        # Replace "uss_bridge__" with "bag__adventure_works__" for reading
        read_filename = filename.replace("uss_bridge__", "bag__adventure_works__")
        target_filename = filename.replace("uss_bridge__", "")
        target_path = os.path.join(target_dir, target_filename)
    
        with open(source_path, "r", encoding="utf-8") as f:
            content = f.read()
    
        # Extract pit_hook column names (to include)
        pit_hooks = sorted(set(pit_hook_pattern.findall(content)))
        # Extract hook column names (to exclude)
        hooks = sorted(set(hook_pattern.findall(content)))
    
        # Prepare the columns to exclude (_hook__ columns)
        if hooks:
            exclude_columns = f"EXCLUDE({', '.join(hooks)})"
        else:
            exclude_columns = ""
    
        if not (pit_hooks or hooks):
            print(f"Skipping {filename} (no hooks found)")
            continue
    
        # Generate SQL content
        sql_content = f"""MODEL (
  kind VIEW
);

SELECT
  * {exclude_columns}
FROM silver.{read_filename.replace('.sql', '')}
"""
    
        # Write to the new target directory
        os.makedirs(target_dir, exist_ok=True)  # Ensure target directory exists
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
    
    print(f"Generated {len(filenames)} uss peripherals in ./models/gold/")
    
if __name__ == "__main__":
    generate_uss_peripherals()