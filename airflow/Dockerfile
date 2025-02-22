FROM apache/airflow:2.10.4 AS builder

USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME


WORKDIR /opt

RUN apt update && apt install -y curl tar unzip && \
    curl -O https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.4-bin-hadoop3.tgz && \
    mv spark-3.5.4-bin-hadoop3 spark && \
    rm -R spark-3.5.4-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH


USER airflow

FROM builder AS builder1
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt