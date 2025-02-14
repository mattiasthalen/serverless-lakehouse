import dlt
import duckdb
import polars as pl

from dotenv import load_dotenv

DUCKDB_PATH = "./lakehouse/lakehouse.duckdb"  # Path to DuckDB file

def get_tables_from_schema(schema: str) -> list:
    """Retrieve all table names from the given DuckDB schema."""
    con = duckdb.connect(DUCKDB_PATH)
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
    tables = con.execute(query).fetchall()
    con.close()
    
    return [table[0] for table in tables]  # Convert list of tuples to list of table names

def read_table_from_duckdb(schema: str, table_name: str):
    """Read a specific table from DuckDB and yield it as a dictionary."""
    con = duckdb.connect(DUCKDB_PATH)
    
    # Load table into Polars
    df = pl.from_pandas(con.execute(f"SELECT * FROM {schema}.{table_name}").fetch_df())
    con.close()

    # Convert binary fields to strings
    casted_df = df.with_columns([
        pl.col(col).cast(pl.Utf8) for col in df.columns if df[col].dtype == pl.Binary
    ])

    yield from casted_df.to_dicts()

def load_schema(schema: str) -> None:
    """Dynamically load all tables from the specified schema into DLT."""
    load_dotenv()
    
    pipeline = dlt.pipeline(
        pipeline_name=f"duckdb__{schema}",
        destination=dlt.destinations.filesystem("./lakehouse"),
        dataset_name=schema,
        progress="enlighten",
        export_schema_path="./pipelines/schemas/export",
        import_schema_path="./pipelines/schemas/import",
        dev_mode=False
    )

    # Get all tables in the schema
    tables = get_tables_from_schema(schema)

    # Generate DLT resources dynamically, with `write_disposition="replace"`
    sources = [
        dlt.resource(
            read_table_from_duckdb(schema, table), 
            name=table, 
            write_disposition="replace"
        ) 
        for table in tables
    ]

    # Run the pipeline for all tables
    load_info = pipeline.run(sources, table_format="delta")
    print(load_info)

if __name__ == "__main__":
    load_schema("silver")
    # load_schema("gold")