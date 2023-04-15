from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# different update mode:
# -> update (only updated results not old) insert + update
# -> complete (full result from all batch)
# -> append (only new results) will not be used in aggregations

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder \
        .appName('Structured Streaming') \
        .master('local[*]') \
        .config('spark.sql.shuffle.partitions', 3) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read data from stream
    df = spark.readStream.format("socket").option('host', 'localhost').option('port', '3131').load()

    # 2. Process the data
    resultDF = df.select(
        f.explode(f.split(f.col('value'), ' '))
        .alias('result')
    ).groupby('result').count().filter(f.trim(f.col('result')) != '')

    # 3. Write the result back to sink
    resultDF.writeStream.format('console').outputMode('update').start().awaitTermination()
