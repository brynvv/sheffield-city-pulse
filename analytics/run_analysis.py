def run_analysis():
    import duckdb

    query = """
        SELECT
            DATE_TRUNC('hour', aq.timestamp) AS hour,
            AVG(aq.pm25) AS avg_pm25,
            AVG(w.temperature) AS avg_temp,
            AVG(w.wind_speed) AS avg_wind
        FROM 'data/raw/air_quality/*/*.parquet' aq
        JOIN 'data/raw/weather/*/*.parquet' w
        ON DATE_TRUNC('hour', aq.timestamp) = DATE_TRUNC('hour', w.timestamp)
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 20
        """

    result = duckdb.sql(query)
    df = result.df()

    print(df.head())
    df.to_parquet("data/processed/city_metrics.parquet", index=False)

if __name__ == "__main__":
    run_analysis()