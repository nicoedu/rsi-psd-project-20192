FROM bde2020/spark-python-template:2.4.4-hadoop2.7

MAINTAINER Nicollas <nicollas.bastos@ufrpe.com.br>

COPY template.sh /
COPY . .

ONBUILD COPY requirements.txt requirements.txt

ONBUILD RUN pip install -r requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION structedSparkHeatIndex.py