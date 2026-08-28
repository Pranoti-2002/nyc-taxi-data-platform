FROM apache/spark:3.5.5

USER root

# PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.7.jar \
    /opt/spark/jars/postgresql-42.7.7.jar

# Hadoop AWS / S3A support
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/hadoop-aws-3.3.4.jar

# AWS SDK required by Hadoop S3A
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 \
    /opt/spark/jars/postgresql-42.7.7.jar \
    /opt/spark/jars/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark