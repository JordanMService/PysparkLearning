import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import  split,date_format, udf, sum, count, col
from pyspark.sql.types import StringType



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
 
#Convert orders to grouped values
ordersDf = ordersDf.groupby("ssid").agg( sum("revenue").alias("revenue"), count("*").alias("transactions"))

#Convert sessions to grouped vals
conversionUdf = udf(convertUnixToDate,StringType())
sessionsDf  = sessionsDf.withColumn("unixTime", split(sessionsDf .ssid,":")[2])
sessionsDf  = sessionsDf.withColumn("startTime",conversionUdf("unixTime"))
sessionsDf  = sessionsDf.withColumn("siteId", split(sessionsDf .ssid,":")[1])

#Join sessions and groups
sessionsAlias = sessionsDf.alias("session")
ordersAlias = ordersDf.alias("order")
sessionOrders = sessionsAlias.join(ordersAlias, ["ssid"])

#Orderby and show values
sessionOrders.groupby("startTime","siteId","gr","browser").agg(count("*").alias("sessions"), sum("transactions").alias("transactions"), sum("revenue").alias("revenue")).show()

#Todo: Ensure we can get the add into these items for the groupby