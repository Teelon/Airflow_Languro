FROM astrocrpublic.azurecr.io/runtime:3.0-14

USER root
RUN apt-get update && apt-get install -y ffmpeg
USER astro

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb==1.8.2 && deactivate