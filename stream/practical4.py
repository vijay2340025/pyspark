from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder \
        .appName('Structured Streaming') \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read data from stream
    df = spark.readStream.format("socket").option('host', 'localhost').option('port', '3131').load()

    # 2. Write the result back to sink
    query = df.writeStream.format('console').outputMode('append').start()
    query.awaitTermination()  # streaming query should be waited till terminated
