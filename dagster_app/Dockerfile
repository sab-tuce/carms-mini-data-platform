FROM python:3.11-slim

WORKDIR /app
COPY dagster_app/requirements.txt /app/dagster_app/requirements.txt
RUN pip install --no-cache-dir -r /app/dagster_app/requirements.txt

COPY dagster_app /app/dagster_app

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

EXPOSE 3000
