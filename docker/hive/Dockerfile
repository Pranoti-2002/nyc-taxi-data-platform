FROM apache/hive:4.2.0

ADD https://jdbc.postgresql.org/download/postgresql-42.7.9.jar \
    /opt/hive/lib/postgresql.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    /opt/hive/lib/hadoop-aws-3.4.1.jar

ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    /opt/hive/lib/aws-sdk-bundle-2.29.52.jar

USER root

RUN chmod 644 \
    /opt/hive/lib/postgresql.jar \
    /opt/hive/lib/hadoop-aws-3.4.1.jar \
    /opt/hive/lib/aws-sdk-bundle-2.29.52.jar

USER hive