# Extends the official Airflow image with the pipeline's Python dependencies.
# Pinning apache-airflow in the same pip call prevents pip from silently
# upgrading or downgrading Airflow while resolving the extra packages.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
