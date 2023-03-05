from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('Basic Streaming')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sparkContext=sc, batchDuration=2)

    stream = ssc.socketTextStream('localhost', 8989)
    word_pairs = stream.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
    word_pairs.reduceByKey(lambda x, y: x + y).pprint()

    ssc.start()
    ssc.awaitTermination()
