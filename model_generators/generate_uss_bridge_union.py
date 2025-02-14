import os

def generate_uss_bridge_union():
    # Path to the folder containing the SQL files
    sql_folder = './models/silver/'
    
    # List to hold the SQL queries
    sql_queries = []
    
    # Loop through the files in the directory
    for filename in os.listdir(sql_folder):
        # Check if the file matches the pattern uss_*_*.sql but skip uss__bridge.sql
        if filename.startswith('uss_') and filename.endswith('.sql') and filename != 'uss__bridge.sql':
            table_name = filename.replace('.sql', '')  # Remove the '.sql' to get the table name
            # Build the SQL query for each file, adding the silver schema and file name
            sql_queries.append(f"SELECT\n  *\nFROM silver.{table_name}")
    
    # Combine the queries with UNION ALL BY NAME, adding a line break before the UNION
    if sql_queries:
        combined_query = "\nUNION ALL BY NAME\n".join(sql_queries)  # Corrected with line break before UNION
    else:
        combined_query = "No matching files found."
    
    # Output the combined query to uss__bridge.sql
    output_file = os.path.join(sql_folder, 'uss__bridge.sql')
    
    # Write the final SQL output with the required format
    final_sql = f"""MODEL (
    kind VIEW
    );
    
    {combined_query}
    """
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_sql)
    
    print(f"Generated {output_file}")