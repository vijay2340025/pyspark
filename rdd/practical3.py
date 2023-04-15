from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('RDD Practical2').setMaster('spark://127.0.0.1:4040')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sparkContext=sc)

if __name__ == '__main__':
    rdd = sc.parallelize([i for i in range(0, 10000)], 1)
    res = rdd.map(lambda x: (x, x * x))
    res.toDF().write.csv("hdfs://vijay:9000/sample")
    sc.stop()
