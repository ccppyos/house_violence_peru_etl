# First-time build can take upto 10 mins.

FROM apache/airflow:2.7.3-python3.9

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq \
    && apt-get install wget -y -qq

COPY requirements.txt .

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID

USER airflow
RUN pip install -q -r requirements.txt

