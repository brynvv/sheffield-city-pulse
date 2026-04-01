import duckdb

result = duckdb.sql("""
SELECT *
FROM 'data/raw/air_quality/*/*.parquet'
""")

print(result.df())