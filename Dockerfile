FROM quay.io/astronomer/astro-runtime:12.8.0

# Install required dependencies
RUN pip install boto3 minio psycopg2-binary apache-airflow-providers-smtp apache-airflow-providers-postgres

# Create directory structure for shared folder and set up mail utilities
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    mailutils \
    sendmail \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /shared_folder/processed /shared_folder/compressed \
    && chmod -R 777 /shared_folder

# Switch back to airflow user
USER astro
