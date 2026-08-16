FROM apache/spark:3.5.5

USER root

ADD https://jdbc.postgresql.org/download/postgresql-42.7.7.jar \
    /opt/spark/jars/postgresql-42.7.7.jar

RUN chmod 644 /opt/spark/jars/postgresql-42.7.7.jar

USER spark