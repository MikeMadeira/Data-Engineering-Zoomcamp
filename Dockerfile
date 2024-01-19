FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data_to_postgres.py ingest_data_to_postgres.py

ENTRYPOINT ["python", "ingest_data_to_postgres.py"]