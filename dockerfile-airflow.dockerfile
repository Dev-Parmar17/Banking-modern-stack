# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt packages
RUN pip install --no-cache-dir dbt-core dbt-snowflake

#  docker compose exec airflow-webserver airflow users create --username dev --firstname dev --lastname p --role Admin --email azure@gmail.com --password pass123