# Our Airflow image: the official Airflow image plus our Python packages.
# We install apache-airflow with the same version in the same pip command.
# This stops pip from changing the Airflow version while it installs the
# other packages.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
