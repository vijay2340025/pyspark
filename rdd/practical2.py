"""
Transformations -> Narrow vs Wide Transformations; eg: map, flatmap and filter

Narrow - No shuffling is involved
(shuffling is mechanism for redistributing or re-partitioning data
so that the data is grouped differently across partitions in the cluster)

Wide - shuffling is involved; eg: reduceByKey, groupByKey (movement of data required as we need to group)

Stages: Marked by shuffle boundaries (whenever we encounter wide transformations we will have one stage)

"""

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
