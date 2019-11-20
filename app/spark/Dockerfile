FROM bde2020/spark-python-template:2.4.4-hadoop2.7


COPY template.sh /
COPY . .

ONBUILD COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && pip install -r requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION structedSparkHeatIndex.py
ENV SPARK_SUBMIT_ARGS '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 --py-files kafka.zip'

CMD ["/bin/bash", "/template.sh"]