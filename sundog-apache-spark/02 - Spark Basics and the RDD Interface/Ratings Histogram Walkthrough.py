# Databricks notebook source
# MAGIC %md
# MAGIC # Taming Big Data with Apache Spark and Python - Hands On!
# MAGIC ## Ratings Histogram Walkthrough
# MAGIC
# MAGIC One of our first tasks is to read a CSV file containing a list of movie ratings data and create a histogram of the rating.  The data set can be found on the GroupLens website at the following address: https://files.grouplens.org/datasets/movielens/ml-25m.zip.  The Udemy course from Sundog education referrs to a version of the data containing 100k rows, however I could only locate a version containing approximately 25 million rows.
# MAGIC
# MAGIC This demo was completed using Databricks instead of a local machine running Apache Spark.  The ratings.csv dataset from GroupLens was first uploaded to Databricks to /FileStore/tables/SundogData and the following code creates a spark DataFrame from the file.
# MAGIC
# MAGIC We're saving ourselves a little bit if compute time by defining our schema upfront.  Instead of reading a portion of the data and inferring the schema, we tell Spark what the data should be.  Also, by defining the schema we enforce some data quality on our data set in the event we receive a different source file that does not adhere to what we expect for the column data types.

# COMMAND ----------

from pyspark.sql.types import *

# Define the schema for the ratings dataset.
schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("movieid", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("epoch_time_seconds", LongType(), True)
])

# Read ratings dataset with defined schema.
df = (spark.read.format("csv")
  .schema(schema)
  .option("header", "true")
  .option("sep", ",")
  .load("/FileStore/tables/SundogData/ratings.csv")
)

# Display the first 5 rows.
display(df.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll use the describe method to calculate some basic summary statistics from our dataframe.  We can tell that our ratings data makes sense since all ratings are from 0.5 to 5.0 in 0.5 point increments.  We can also tell that the size of our dataframe is close to the 25 million records we expect from the count data.

# COMMAND ----------

# Calculate a description of the dataframe.
df.describe().show()

# COMMAND ----------

(df
 .write
 .format("parquet")
 .mode("overwrite")
 .saveAsTable("sundogdata.movielens_ratings"))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll group the data by rating and get a count by rating.  We could use Databricks to create the visualization, but instead let's convert the Spark DataFrame to pandas to manually create out plot.

# COMMAND ----------

from pyspark.sql.functions import col

rating_counts = (df.groupBy("rating")
    .count()
    .sort(col("rating"))
    .toPandas()
)

display(rating_counts)

# COMMAND ----------

rating_counts.plot.bar(x="rating", y="count")

# COMMAND ----------

# MAGIC %md
# MAGIC The data is left skewed; it seems our users are pretty generous when rating a movie.  4.0 is the most common movie rating, and ratings 0.5 to 2.5 are the least common ratings.
