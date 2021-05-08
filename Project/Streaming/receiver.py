import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import global_vals
from mongo_utils import mongo_utils

def insert_row(x):
    if x is None or len(x)<1:
        return
    data_list=x.split(',')
    mongo_utils.insert_data({
                "datetime":data_list[0],
                "asset_id":data_list[1],
                "Voltage": data_list[2],
                "Current": data_list[3],
                "Power Active": data_list[4],
                "Power Apparent": data_list[5],
                "Power Reactive": data_list[6],
                "Power Factor": data_list[7],
                "Frequency": data_list[8],
                "Imported Energy Active": data_list[9],
                "Exported Energy Active": data_list[10],
                "Imported Energy Reactive": data_list[11],
                "Exported Energy Reactive": data_list[12],
                "Total Energy Active": data_list[13],
                "Total Energy Reactive": data_list[14]
    })


def update_row(x):
    if x is None or len(x)<1:
        return
    data_list=x.split(',')
    mongo_utils.update_data({
                "datetime":data_list[0],
                "asset_id":data_list[1],
                "Voltage": data_list[2],
                "Current": data_list[3],
                "Power Active": data_list[4],
                "Power Apparent": data_list[5],
                "Power Reactive": data_list[6],
                "Power Factor": data_list[7],
                "Frequency": data_list[8],
                "Imported Energy Active": data_list[9],
                "Exported Energy Active": data_list[10],
                "Imported Energy Reactive": data_list[11],
                "Exported Energy Reactive": data_list[12],
                "Total Energy Active": data_list[13],
                "Total Energy Reactive": data_list[14]
    })

sc=SparkContext(master='local[*]',appName='test')
# topic="topic_test"
# .config("spark.jars", "/path/to/jar.jar,/path/to/another/jar.jar") \
ssc=StreamingContext(sc,batchDuration=20)
brokers='*.*.*.*:9092'
# brokers='172.30.0.2:9093'

topic=global_vals.kafka_topic
kvs=KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list":brokers})
kvs.pprint()


lines=kvs.map(lambda x:'{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(
# lines=kvs.map(lambda x:'{}'.format(
    json.loads(x[1])['datetime'],
    json.loads(x[1])['asset_id'],
    json.loads(x[1])['Voltage'],
    json.loads(x[1])['Current'],
    json.loads(x[1])['Power Active'],
    json.loads(x[1])['Power Apparent'],
    json.loads(x[1])['Power Reactive'],
    json.loads(x[1])['Power Factor'],
    json.loads(x[1])['Frequency'],
    json.loads(x[1])['Imported Energy Active'],
    json.loads(x[1])['Exported Energy Active'],
    json.loads(x[1])['Imported Energy Reactive'],
    json.loads(x[1])['Exported Energy Reactive'],
    json.loads(x[1])['Total Energy Active'],
    json.loads(x[1])['Total Energy Reactive']
    ))

lines.foreachRDD(lambda rdd:rdd.foreach(insert_row))
lines.foreachRDD(lambda rdd:rdd.foreach(update_row))
ssc.start()
ssc.awaitTermination()