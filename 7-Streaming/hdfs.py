# import os
# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
# import json
# sc=SparkContext(master='local[*]',appName='test')
# ssc=StreamingContext(sc,1)
# brokers='192.168.0.103:9092'
# topic="topic_test"


# lines=ssc.textFileStream("hdfs://localhost:9000/user/pratham/demo.txt")
# # counts = lines.flatMap(lambda line: line.split(" "))\
# #                 .map(lambda x: (x, 1))\
# #                 .reduceByKey(lambda a, b: a+b)
# lines.pprint()
# ssc.start()
# ssc.awaitTermination()

# # spark-submit /home/pratham/PersonalSpace/30-days-of-pyspark/7-Streaming/hdfs.py


import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 20)

    lines = ssc.textFileStream(sys.argv[1])
    words = lines.flatMap(lambda x : x.split(' '))
    wordCounts = words.map(lambda x :  (x, 1)).reduceByKey(lambda x,y : x+y)
    wordCounts.saveAsTextFiles(sys.argv[2])
    ssc.start()
    ssc.awaitTermination()

    # ssc.start()
    # ssc.awaitTermination()