# Use a minimal Python base image
FROM python:3.10-slim

# Set up a virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy requirements.txt before installing dependencies
COPY requirements.txt .

# Install dependencies using requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Set Dagster home directory
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME
RUN mkdir -p $DAGSTER_HOME/data

# Copy Dagster configuration files
COPY dagster_home/dagster.yaml dagster_home/workspace.yaml dagster_home/repository.py $DAGSTER_HOME
COPY dagster_home/data /opt/dagster/dagster_home/data

WORKDIR $DAGSTER_HOME