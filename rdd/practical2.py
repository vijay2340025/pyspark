from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('RDD Practical2').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    l = [
        "WARN: Tuesday 4 September 0465",
        "WARN: Tuesday 4 September 0465",
        "WARN: Tuesday 4 September 0465",
        "ERROR: Tuesday 4 September 0465",
        "ERROR: Tuesday 4 September 0465",
    ]
    rdd = sc.parallelize(l, 2)

    res = rdd.map(lambda x: (x.split(":")[0], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .collect()

    for i in res:
        print(i)

    sc.stop()
