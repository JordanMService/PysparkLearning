import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import  split,date_format, udf
from pyspark.sql.types import *



def checkNullCol(df):
    columnNames = df.schema.names
    for name in columnNames :
        nullCount = df.filter(df[name].isNull() | (df[name] == "")).count()
        print("Null %s count %d" % (name, nullCount))

def convertUnixToDate(unixStr):
    unix = int(unixStr)
    dt = (datetime.datetime.fromtimestamp(unix)).replace(minute=0, second=0)
    dtStr = (dt.strftime("%Y-%m-%d %H:00 (UTC)"))
    return dtStr

def ssidToStartTime(ssid) :
    splitArray = ssid.split(":")
    return splitArray[2]

def withStartTime(df) :
    df.withColumn("startTime",)

sc = SparkContext("local","first app")
sqlContext = SQLContext(sc)

ordersRdd = sc.textFile("data/orders.gz")
ordersDf = sqlContext.read.json(ordersRdd)
#checkNullCol(ordersDf)

featuresRdd = sc.textFile("data/features.gz")
featuresDf = sqlContext.read.json(featuresRdd)
#checkNullCol(featuresDf)


sessionsRdd = sc.textFile("data/sessions.gz")
sessionsDf = sqlContext.read.json(sessionsRdd)
#checkNullCol(sessionsDf)
 

ordersAlias = ordersDf.alias("data/order")
featuresAlias = featuresDf.alias("feature")
sessionsAlias = sessionsDf.alias("session")


conversionUdf = udf(convertUnixToDate,StringType())
sessionsDf = sessionsDf.withColumn("unixTime", split(sessionsDf.ssid,":")[2])
startTimes = sessionsDf.select("unixTime",conversionUdf("unixTime").alias("startTimes"))
startTimes.select('startTimes').distinct().show()