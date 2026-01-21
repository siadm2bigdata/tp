FROM bitnami/spark:3.5.0

USER root
RUN pip install pandas pyarrow

WORKDIR /app
COPY job_spark.py /app/job_spark.py
