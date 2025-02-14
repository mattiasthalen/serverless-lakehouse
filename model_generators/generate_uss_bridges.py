import os
import re

def generate_uss_bridges():
    # Define path
    source_dir = "./models/silver"
    
    # Regular expressions to capture _pit_hook__ and _hook__ column names
    pit_hook_pattern = re.compile(r"(_pit_hook__\w+)")
    hook_pattern = re.compile(r"(_hook__\w+)")
    
    # Process each SQL file in the source directory
    for filename in os.listdir(source_dir):
        if filename.startswith("uss__int__") and filename.endswith(".sql"):
            source_path = os.path.join(source_dir, filename)
            target_filename = filename.replace("uss__int__", "uss__bridge__")
            target_path = os.path.join(source_dir, target_filename)
    
            with open(source_path, "r", encoding="utf-8") as f:
                content = f.read()
    
            # Extract hook column names
            pit_hooks = sorted(set(pit_hook_pattern.findall(content)))
            hooks = sorted(set(hook_pattern.findall(content)))
    
            if not (pit_hooks or hooks):
                print(f"Skipping {filename} (no hooks found)")
                continue
    
            # Extract base & stage name 
            base_name = filename.replace(".sql", "")
            stage_name = base_name.split("__", 1)[1]
    
            # Generate SQL content
            sql_content = f"""MODEL (
    kind VIEW
    );
    
    SELECT
        '{stage_name}' AS stage,
        {',\n    '.join(pit_hooks + hooks)},    
        _sqlmesh__loaded_at,
        _sqlmesh__version,
        _sqlmesh__valid_from,
        _sqlmesh__valid_to,
        _sqlmesh__is_current,
        _sqlmesh__updated_at
    FROM silver.{base_name}
    """
    
            # Write to new file
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(sql_content)
    
            print(f"Generated {target_path}")
    
    print("Done.")