FROM apache/spark:3.3.2

USER root

# Install necessary tools
RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Copy the application JAR
COPY build/libs/cur-java-spark-*.jar /opt/spark/jars/app.jar

# Create configuration directory
RUN mkdir -p /etc/cur-ingestion

# Set the working directory
WORKDIR /opt/spark

# Set default command - run the CUR Ingestion App with the config file
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "com.example.CURIngestionApp"]
CMD ["--master", "local[*]", "/opt/spark/jars/app.jar", "/etc/cur-ingestion/config.yaml"]
