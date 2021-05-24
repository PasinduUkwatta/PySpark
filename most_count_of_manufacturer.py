

from pyspark.sql import SparkSession

# Create a SparkSession

spark = SparkSession.builder.appName("MostCountManufacturer").getOrCreate()

# Load up data as dataframe
data = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("C:/Users/Acer/Desktop/BigDataSet/vehicles_final.csv")

# data.printSchema()

sortdata=data.select('manufacturer').na.drop(subset=["manufacturer"]).groupBy("manufacturer").count().sort("count",ascending=False)


sortdata.show()


