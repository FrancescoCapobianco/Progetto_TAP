FROM apache/spark:3.5.3

USER root

RUN pip install --upgrade pip
RUN pip install numpy