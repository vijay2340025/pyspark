from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('practical1')
sc = SparkContext(conf=conf)

if __name__ == '__main__':
    l = [1, 2, 3]

    sc.parallelize(l).map(lambda x: x * x).foreach(lambda x: print(x))
