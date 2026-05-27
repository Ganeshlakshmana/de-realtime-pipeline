FROM apache/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt /opt/spark-app/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark-app/requirements.txt

# Copy Spark job
COPY streaming_job.py /opt/spark-app/streaming_job.py

# Kafka + PostgreSQL JARs (Spark 3.5 / Scala 2.12)
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Create checkpoint directory with correct permissions before switching user
RUN mkdir -p /tmp/spark-checkpoint && chmod -R 777 /tmp/spark-checkpoint

USER 185

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "/opt/spark-app/streaming_job.py"]