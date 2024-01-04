FROM apache/spark:3.5.0

ENV PATH="${PATH}:/opt/spark/bin"

USER root
RUN mkdir -p /home/spark; chown spark:spark /home/spark
USER spark
