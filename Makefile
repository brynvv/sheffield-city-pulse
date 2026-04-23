install:
	pip install -r requirements.txt

run-air-quality:
	python pipelines/ingest_air_quality.py

run-weather:
	python pipelines/ingest_weather.py

run-analytics:
	python analytics/run_analysis.py

run-pipeline:
	run-air-quality
	run-weather
	run-analytics

clean:
	rm -rf data/raw/*
	rm -rf data/processed/*
	rm -rf logs/*

run-dev:
	clean run-pipeline

