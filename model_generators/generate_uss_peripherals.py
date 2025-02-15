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
    for filename in os.listdir(source_dir):
        if filename.startswith("uss__int__") and filename.endswith(".sql"):
            source_path = os.path.join(source_dir, filename)
            
            # Split the filename by '__' and take the last part for the target filename
            parts = filename.replace("uss__int__", "").split("__")
            target_filename = parts[-1]  # No additional .sql here
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
FROM silver.{filename.replace('.sql', '')}
"""
    
            # Write to the new target directory
            os.makedirs(target_dir, exist_ok=True)  # Ensure target directory exists
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(sql_content)
    
            print(f"Generated {target_path}.sql")
    
    print("Done.")
    
if __name__ == "__main__":
    generate_uss_peripherals()