FROM apache/spark:3.3.2

USER root

# Install necessary tools
RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Copy the application JAR
COPY target/cur-java-spark-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/spark/jars/

# Set the working directory
WORKDIR /opt/spark

# Set default command
CMD ["sh", "-c", "sleep infinity"]
