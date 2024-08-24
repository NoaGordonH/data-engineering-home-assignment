from pyspark.python.pyspark.shell import spark
from pyspark.sql import Window
from pyspark.sql.functions import avg, round, col, format_number, max, lag, last, stddev, sqrt, lit

# Assumption: the note of missing price on a given date doesn't mean fill the gap between dates.

if __name__ == '__main__':
    sdf = spark.read.csv(path='stocks_data.csv',  header=True, inferSchema=True).withColumnRenamed('Date', 'date')
    window = Window.partitionBy("ticker").orderBy("date")
    sdf = sdf.withColumn("close",last("close", ignorenulls=True).over(window))
    sdf = sdf.withColumn("prev_close", lag("close").over(window))
    sdf = sdf.withColumn("return",(col("close") - col("prev_close")) / col("prev_close") * 100)

    sdf.show()

    sdf_1 = sdf.groupby('date').agg(round(avg("return"), 3).alias("average_return"))

    sdf_1.show()

    sdf_2 = sdf.withColumn('worth', col('close')*col('volume'))
    sdf_2 = sdf_2.groupby('ticker').agg(avg('worth').alias('worth_avg'))
    max_value = sdf_2.agg(max("worth_avg")).collect()[0][0]
    sdf_2 = sdf_2.filter(col("worth_avg") == max_value).withColumn('value', format_number('worth_avg', 3))
    sdf_2 = sdf_2.select('ticker', 'value')

    sdf_2.show()

    sdf_3 = sdf.groupby('ticker').agg(round(stddev("return"), 3).alias("standard_deviation"))
    sdf_3 = sdf_3.withColumn('standard_deviation', col('standard_deviation')*sqrt(lit(252)))
    sdf_3 = sdf_3.orderBy(col('standard_deviation').desc()).limit(1)

    sdf_3.show()

    sdf_4 = sdf.withColumn("prev_30_close", lag("close", 30).over(window))
    sdf_4 = sdf_4.withColumn("return_30",(col("close") - col("prev_30_close")) / col("prev_30_close") * 100)
    sdf_4 = sdf_4.orderBy(col('return_30').desc()).limit(3)
    sdf_4 = sdf_4.select('ticker', 'date')

    sdf_4.show()
