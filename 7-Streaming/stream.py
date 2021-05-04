import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
sc=SparkContext(master='local[*]',appName='test')
ssc=StreamingContext(sc,batchDuration=20)
brokers='192.168.0.103:9092'
topic="topic_test"
kvs=KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list":brokers})
kvs.pprint()
ssc.start()
ssc.awaitTermination()

# spark-submit --jars /home/pratham/PersonalSpace/30-days-of-pyspark/7-Streaming/spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar --py-files /home/pratham/PersonalSpace/30-days-of-pyspark/7-Streaming/mongo_driver.py /home/pratham/PersonalSpace/30-days-of-pyspark/7-Streaming/stream_etl_mongo.py
