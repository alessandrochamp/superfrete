# Dockerfile
FROM apache/airflow:2.10.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
ENTRYPOINT ["/entrypoint"]
CMD ["airflow", "webserver"]
