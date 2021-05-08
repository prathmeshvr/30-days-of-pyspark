


from pyspark.sql.functions import *


import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
# load mongo data
input_uri = "mongodb+srv://cdac:cdac@cluster0.fa505.mongodb.net/DB.logs"
output_uri = "mongodb+srv://cdac:cdac@cluster0.fa505.mongodb.net/DB.yoo"

my_spark = SparkSession    .builder    .appName("MyApp")    .config("spark.mongodb.input.uri", input_uri)    .config("spark.mongodb.output.uri", output_uri)    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.4.2')    .getOrCreate()
df = my_spark.read.format('mongo').load()
# print(df.show())



df.printSchema()


#let's change the data type to a timestamp
df = df.withColumn("datetime", df.datetime.cast("long"))
df = df.withColumn("datetime", df.datetime.cast("timestamp"))
# df = df.withColumn("current_date",current_date())


df.printSchema()



df2=df.withColumn('datetime',to_timestamp(col('datetime')))  .withColumn('end_timestamp', current_timestamp())  .withColumn('DiffInSeconds',col("end_timestamp").cast("long") - col('datetime').cast("long"))
  
df2.select(['datetime','current_date','DiffInSeconds']).show()





df2=df2.withColumn('DiffInDays',round(col('DiffInSeconds')/86400))
df2.select(['datetime','current_date','DiffInSeconds','DiffInDays']).show()



cols = ("current_date","DiffInSeconds")

df2.drop(*cols)    .printSchema()



df3=df2.filter("DiffInDays == 1")
df3.select(['datetime','current_date','DiffInSeconds','DiffInDays']).show()



#Here we are calculating a moving average
from pyspark.sql.window import Window
from pyspark.sql import functions as F



movAvg = df3.withColumn("movingAverage", F.avg("Total Energy Active").over( Window.partitionBy("asset_id").rowsBetween(-1,1)) )
# movAvg.show()
colss = ("DiffInDays","end_timestamp","DiffInSeconds")
movAvg=movAvg.drop(*colss)



movAvg.write.format("mongo").mode("append").save()
# working




