## 2. From RDDs to DataFrames ##

sample_lines = []
with open('census_2010.json') as f:
    for i in range(10):
        a = f.readline().strip()
        sample_lines.append(a)
        
print(sample_lines)

## 3. Loading JSON Data into a DataFrame ##

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("CensusAnalysis") \
.getOrCreate()

df = spark.read.json('census_2010.json')
df.show(10)

## 4. Understanding the DataFrame Schema ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")

df.printSchema()
column_names = df.columns
column_types = df.dtypes

print(column_names,'\n',column_types)

## 5. Selecting Columns ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")

age_dot = df.age
age_bracket = df["age"]
print(age_dot)

age_selected = df.select('age')
age_selected.show(5)

## 7. Creating New Columns ##

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")

df_with_ratio = df \
.withColumn("female_male_ratio", col('females')/col('males'))

ratio_preview = df_with_ratio \
.select('age', 'females', 'males', 'female_male_ratio')

ratio_preview.show(10)

## 8. Filtering Rows ##

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")
df_with_ratio = df.withColumn("female_male_ratio", col("females") / col("males"))

female_majority_df = df_with_ratio \
.filter(col('female_male_ratio')>1.0)

female_majority_preview = female_majority_df \
.select('age','females','males','female_male_ratio')

female_majority_preview.show(10)

## 9. Aggregating Data ##

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")
df_with_ratio = df.withColumn("female_male_ratio", col("females") / col("males"))

df_categorized = df_with_ratio \
.withColumn('age_group',
    when(col('age')<18 , 'Minor')
    .when((col('age')>18) & (col('age')<64), 'Working Age')
    .otherwise("Retirement Age")
)

age_group_totals = df_categorized \
.groupBy('age_group') \
.agg(sum("total").alias('group_population'))

age_group_totals.show()

## 10. Chaining DataFrame Operations ##

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")
df_with_ratio = df.withColumn("female_male_ratio", col("females") / col("males"))

youth_analysis = df_with_ratio \
.withColumn("gender_gap", col("females") - col("males")) \
.filter(col('age')<18) \
.select('age', 'total', 'gender_gap', 'female_male_ratio') \
.orderBy(col('age'), ascending=False)

youth_analysis.show(10)

## 11. Converting DataFrames to Pandas for Visualization ##

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CensusAnalysis").getOrCreate()
df = spark.read.json("census_2010.json")

# Create categories and calculate average ratios per category
category_ratios = (df
    .withColumn("female_male_ratio", col("females") / col("males"))
    .withColumn("age_group", 
        when(col("age") < 18, "Minor")
        .when(col("age") < 65, "Working Age")
        .otherwise("Retirement Age"))
    .groupBy("age_group")
    .agg(avg("female_male_ratio").alias("avg_ratio"))
)

category_pandas = category_ratios.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(category_pandas['age_group'], category_pandas['avg_ratio'])
plt.xlabel('age')
plt.ylabel('avg_ratio')
plt.axhline(y=1.0, color='red', linestyle='--', alpha=0.7)
plt.title('Average Female-to-Male Ratio by Life Stage')
plt.grid(True, alpha=0.3)
plt.show()