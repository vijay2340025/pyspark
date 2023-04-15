from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder \
        .appName('Structured Streaming') \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read data from stream
    df = spark.readStream.format("socket").option('host', 'localhost').option('port', '3131').load()

    # 2. Process the data
    resultDF = df.select(
        f.explode(f.split(f.col('value'), ' '))
        .alias('result')
    ).groupby('result').count()

    # 3. Write the result back to sink
    query = resultDF.writeStream.format('console').outputMode('complete').start()
    query.awaitTermination()
