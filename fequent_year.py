from pyspark.sql import SparkSession

# Create a SparkSession

spark = SparkSession.builder.appName("FrequentYear").getOrCreate()

# Load up data as dataframe
data = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("C:/Users/Acer/Desktop/BigDataSet/vehicles_final.csv")

# data.printSchema()

sortdata=data.select('year').na.drop(subset=["year"]).groupBy("year").count().sort("count",ascending=False)


sortdata.show()

