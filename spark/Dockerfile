FROM bitnami/spark:latest
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --timeout=1000 -r /tmp/requirements.txt
USER 1001