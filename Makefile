setup-env:
	python -m venv venv
	powershell.exe .\venv\Scripts\Activate.ps1
	pip install dagster dagster-webserver pandas duckdb
	dagster project scaffold
