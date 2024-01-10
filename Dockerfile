FROM apache/airflow:2.8.0

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.8.0

USER root
RUN apt-get update -qq && apt-get install vim -qqq

RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

USER $AIRFLOW_UID

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

WORKDIR $AIRFLOW_HOME