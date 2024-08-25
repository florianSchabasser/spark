FROM openjdk:8-jre-slim

ARG SPARK_VERSION=3.5.3

# Copy the Spark .tgz file
COPY spark-${SPARK_VERSION}.5-bin-spark-lineage.tgz /tmp/

# Install and set up Spark
RUN tar -xzf /tmp/spark-${SPARK_VERSION}.5-bin-spark-lineage.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-spark-lineage /opt/spark && \
    rm /tmp/spark-${SPARK_VERSION}.5-bin-spark-lineage.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR $SPARK_HOME