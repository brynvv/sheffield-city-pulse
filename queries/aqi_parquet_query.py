import duckdb

result = duckdb.sql("""
SELECT *
FROM 'data/processed/*.parquet'
""")

print(result.df())