FROM flink:1.18.0

USER root

ARG FLINK_KAFKA_CONNECTOR_VERSION=3.0.1-1.18

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/
RUN chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar

USER flink
