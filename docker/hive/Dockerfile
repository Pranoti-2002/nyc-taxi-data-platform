FROM apache/hive:4.2.0

ADD https://jdbc.postgresql.org/download/postgresql-42.7.9.jar /opt/hive/lib/postgresql.jar

USER root
RUN chmod 644 /opt/hive/lib/postgresql.jar
USER hive