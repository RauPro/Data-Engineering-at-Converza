FROM apache/airflow:2.9.3-python3.11

# Install project dependencies with uv as the 'airflow' user
USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
COPY requirements.txt /opt/airflow/requirements.txt

# Create and use a dedicated virtual environment for project deps
RUN uv venv /home/airflow/.venv
ENV VIRTUAL_ENV="/home/airflow/.venv"
ENV PATH="/home/airflow/.venv/bin:${PATH}"
RUN uv pip install -r /opt/airflow/requirements.txt

# Airflow runtime configuration
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

WORKDIR /opt/airflow


