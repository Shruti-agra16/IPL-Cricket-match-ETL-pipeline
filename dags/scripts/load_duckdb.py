# import duckdb
# import os

# # Path to your gold parquet files (downloaded from Databricks later)
# GOLD_DIR = os.path.join(os.path.dirname(__file__), '..', 'gold_parquet')

# # Path where DuckDB will save your warehouse file
# DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'ipl_warehouse.duckdb')

# # Connect to DuckDB (creates the file if it doesn't exist)
# con = duckdb.connect(DB_PATH)

# # Load each Gold table from Parquet files
# tables = {
#     'top_batsmen':  f'{GOLD_DIR}/top_batsmen/*.parquet',
#     'bowler_phase': f'{GOLD_DIR}/bowler_phase/*.parquet',
#     'toss_impact':  f'{GOLD_DIR}/toss_impact/*.parquet',
#     'team_venue':   f'{GOLD_DIR}/team_venue/*.parquet',
# }

# for table_name, path in tables.items():
#     con.execute(f"""
#         CREATE OR REPLACE TABLE {table_name} AS
#         SELECT * FROM read_parquet('{path}')
#     """)
#     count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
#     print(f'  {table_name}: {count:,} rows loaded')

# con.close()
# print(f'\nDuckDB warehouse saved at: {DB_PATH}')


