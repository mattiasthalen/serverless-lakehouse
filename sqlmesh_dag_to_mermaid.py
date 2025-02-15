import json
import os
import re
import subprocess
import sys
import tempfile

from bs4 import BeautifulSoup

def run_sqlmesh_dag():
    """Run sqlmesh dag command and save output to a temporary file"""
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        # Run sqlmesh dag command with the temp file as output
        subprocess.run(['sqlmesh', 'dag', temp_path], check=True)
        
        # Read the contents of the temp file
        with open(temp_path, 'r') as f:
            content = f.read()
            
        # Clean up the temporary file
        os.unlink(temp_path)
        
        return content
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlmesh dag: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def extract_graph_data(html_content):
    """Extract nodes and edges from the HTML output"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the JavaScript code that contains the graph data
    script = soup.find('script', string=re.compile('vis.DataSet'))
    if not script:
        print("No script found with vis.DataSet")
        return None, None
    
    # Extract nodes and edges data using regex
    nodes_match = re.search(r'nodes = new vis\.DataSet\((.*?)\)', script.string, re.DOTALL)
    edges_match = re.search(r'edges: new vis\.DataSet\((.*?)\)', script.string, re.DOTALL)
    
    if not nodes_match or not edges_match:
        print("Could not find nodes or edges data")
        return None, None
    
    try:
        nodes = json.loads(nodes_match.group(1))
        edges = json.loads(edges_match.group(1))
        return nodes, edges
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None, None

def get_db_and_schema_from_id(node_id):
    """Extract database and schema names from node ID"""
    parts = node_id.split('.')
    if len(parts) >= 3:
        return parts[0].strip('"'), parts[1].strip('"')
    return None, None

def get_schema_order(schema):
    """Helper function to determine schema order"""
    order = {
        'bronze': 0,
        'silver': 1,
        'gold': 2
    }
    return order.get(schema, 999)  # Unknown schemas go to the end

def convert_to_mermaid(nodes, edges):
    """Convert nodes and edges to Mermaid flowchart format"""
    mermaid_code = ["flowchart LR"]
    
    # Group nodes by database and schema
    db_schemas = {}
    for node in nodes:
        db, schema = get_db_and_schema_from_id(node['id'])
        if db and schema:
            db_schema_key = f"{db}.{schema}"
            if db_schema_key not in db_schemas:
                db_schemas[db_schema_key] = []
            node_name = node['label'].strip('"')
            db_schemas[db_schema_key].append(node_name)
    
    # Add subgraphs for each schema (including database name)
    for db_schema in sorted(db_schemas.keys(), key=lambda x: get_schema_order(x.split('.')[-1])):
        mermaid_code.append(f"    subgraph {db_schema}[\"{db_schema}\"]")
        mermaid_code.append("        direction LR")
        for node in sorted(db_schemas[db_schema]):
            node_id = node.replace('.', '_').replace('-', '_')
            mermaid_code.append(f"        {node_id}([\"{node}\"])")
        mermaid_code.append("    end")
        mermaid_code.append("")
    
    # Group edges by source and target database.schema
    edge_groups = {}
    for edge in edges:
        from_parts = edge['from'].split('.')
        to_parts = edge['to'].split('.')
        from_db_schema = f"{from_parts[0].strip('\"')}.{from_parts[1].strip('\"')}"
        to_db_schema = f"{to_parts[0].strip('\"')}.{to_parts[1].strip('\"')}"
        group_key = f"{from_db_schema} -> {to_db_schema}"
        
        from_node = edge['from'].split('.')[-1].strip('"').replace('.', '_').replace('-', '_')
        to_node = edge['to'].split('.')[-1].strip('"').replace('.', '_').replace('-', '_')
        
        if group_key not in edge_groups:
            edge_groups[group_key] = []
        edge_groups[group_key].append(f"    {from_node} --> {to_node}")
    
    # Add grouped relationships with comments in correct order
    for group_key in sorted(edge_groups.keys(), key=lambda x: (
            get_schema_order(x.split(' -> ')[0].split('.')[-1]),
            get_schema_order(x.split(' -> ')[1].split('.')[-1]))):
        mermaid_code.append(f"    %% {group_key}")
        mermaid_code.extend(sorted(edge_groups[group_key]))
        mermaid_code.append("")
    
    return "\n".join(mermaid_code)

def main():
    # Get output path if provided
    output_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run sqlmesh dag and get output
    html_output = run_sqlmesh_dag()
    if not html_output:
        return
    
    # Extract nodes and edges
    nodes, edges = extract_graph_data(html_output)
    if not nodes or not edges:
        print("Failed to extract graph data")
        return
    
    # Convert to Mermaid
    mermaid_code = convert_to_mermaid(nodes, edges)
    
    if output_path:
        if '/' in output_path or '\\' in output_path:
            # If path contains separators, create directories
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # Save to specified file
        with open(output_path, 'w') as f:
            f.write(mermaid_code)
    else:
        # Print to stdout
        print(mermaid_code)

if __name__ == "__main__":
    main()