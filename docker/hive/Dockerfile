FROM apache/hive:3.1.3

ADD https://jdbc.postgresql.org/download/postgresql-42.7.9.jar \
    /opt/hive/lib/postgresql.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar \
    /opt/hive/lib/hadoop-aws-3.1.0.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar \
    /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

USER root

RUN find /opt/hadoop/share/hadoop /opt/tez/lib \
    -type f -name 'guava-*.jar' -delete
RUN rm -f \
    /opt/tez/lib/hadoop-mapreduce-client-common-2.7.0.jar \
    /opt/tez/lib/hadoop-mapreduce-client-core-2.7.0.jar

RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar

RUN mkdir -p /tmp/hive/warehouse \
    && chown hive:hive /tmp/hive /tmp/hive/warehouse \
    && chmod 733 /tmp/hive /tmp/hive/warehouse

RUN chmod 644 \
    /opt/hive/lib/postgresql.jar \
    /opt/hive/lib/hadoop-aws-3.1.0.jar \
    /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

USER hive