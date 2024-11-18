FROM python:3.10-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

RUN pip install dlt dagster dagster-webserver

# Copy your code and workspace to /opt/dagster/app
COPY mongo_postgres /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Copy dagster instance YAML to $DAGSTER_HOME
COPY dagster.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

RUN pip install -r mongo_postgres/requirements.txt

EXPOSE 3000

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]