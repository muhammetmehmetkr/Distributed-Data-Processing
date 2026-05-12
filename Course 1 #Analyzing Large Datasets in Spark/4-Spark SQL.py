# Registering DataFrames as SQL Views
from pyspark.sql import SparkSession

# Start a Spark session
spark = SparkSession.builder.getOrCreate()

# Load the JSON data into a DataFrame
df = spark.read.json("census_2010.json")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("census2010")

tables_result = spark.sql("SHOW TABLES")
result = spark.sql("SELECT total, year FROM census2010")
result.show(5)

# Filtering
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("census_2010.json")
df.createOrReplaceTempView("census2010")

result = spark.sql("""
SELECT age, females, males 
FROM census2010
WHERE (age BETWEEN 0 and 9) OR (age>=91)
""")
result.show()

# Creating New Columns with SQL Expressions
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("census_2010.json")
df.createOrReplaceTempView("census2010")

result = spark.sql("""
SELECT age, females, males, ROUND(females/males,4) as female_male_ratio
FROM census2010
""")
result.show(5)

# Sorting Results
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("census_2010.json")
df.createOrReplaceTempView("census2010")

result = spark.sql("""
SELECT age, total, females, ROUND(females/males,3) as female_male_ratio
FROM census2010
ORDER BY female_male_ratio ASC
""")
result.show()

# Aggregating Data with SQL Functions
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("census_2010.json")
df.createOrReplaceTempView("census2010")

result  = spark.sql("""
SELECT MAX(total) as max_population,
MIN(total) as min_population,
COUNT(*) as total_age_groups
FROM census2010

""")

result.show()

# Merging Multiple Datasets
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_1980 = spark.read.json("census_1980.json")
df_1990 = spark.read.json("census_1990.json")
df_2000 = spark.read.json("census_2000.json")
df_2010 = spark.read.json("census_2010.json")


df_1980.createOrReplaceTempView("census1980")
df_1990.createOrReplaceTempView("census1990")
df_2000.createOrReplaceTempView("census2000")
df_2010.createOrReplaceTempView("census2010")

merged = spark.sql("""
SELECT * FROM census1980
UNION ALL
SELECT * FROM census1990
UNION ALL
SELECT * FROM census2000
UNION ALL
SELECT * FROM census2010
""")
merged.createOrReplaceTempView("census_all_decades")

result = spark.sql("""
SELECT year, age, total FROM census_all_decades
WHERE age=0
ORDER BY year desc
""")
result.show()

# Grouping Data
# The four decades of census data have been merged into a temporary view called "census_all_decades"
result = spark.sql("""
SELECT year, SUM(females) as total_females,
AVG(males) as avg_males
FROM census_all_decades
GROUP BY year
ORDER BY year desc
""")
result.show()

# Building a Full SQL Pipeline
# The four decades of census data are available in the "census_all_decades" view
result = spark.sql("""
SELECT year, SUM(females) as total_females,
SUM(males) as total_males,
ROUND(SUM(females)/SUM(males),4) as female_male_ratio
FROM census_all_decades
WHERE males>females
GROUP BY year
ORDER BY year desc
""")
result.show()