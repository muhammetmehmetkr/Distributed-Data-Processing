## 2. Resilient Distributed Dataset ##

from pyspark.sql import SparkSession

# Create RDDs from different sources
spark = SparkSession.builder \
.appName('population-analysis') \
.getOrCreate()

population_rdd = spark.sparkContext.textFile("world_population.csv")

my_list = [10, 20, 30, 40, 50]
numbers_rdd = spark.sparkContext.parallelize(my_list)

sample_lines = population_rdd.take(3)
sample_numbers = numbers_rdd.take(3)

print(sample_lines)
print(sample_numbers)

## 3. Removing the Header ##

from pyspark.sql import SparkSession

# Explore and clean the population data

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")

total_rows = population_rdd.count()
header = population_rdd.first()

clean_data = population_rdd.filter(lambda line: line != header)

data_rows = clean_data.count()
preview_data = clean_data.take(5)

print(header)
print(total_rows)
print(data_rows)
print(preview_data)

## 4. Transforming Data ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")
header = population_rdd.first()
clean_data = population_rdd.filter(lambda line: line != header)

# Transform raw strings into structured data
split_data = clean_data.map(lambda line: line.split(','))
country_population = split_data.map(lambda fields: (fields[0], int(fields[1])))

country_count = country_population.count()
sample_structured = country_population.take(5)

print(country_count)
print(sample_structured)

## 5. Filtering and Data Quality ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")
header = population_rdd.first()
clean_data = population_rdd.filter(lambda line: line != header)
split_data = clean_data.map(lambda line: line.split(','))
country_population = split_data.map(lambda fields: (fields[0], int(fields[1])))

# Filter countries by population size
large_countries = country_population.filter(lambda population: population[1]>50000000)
small_countries = country_population.filter(lambda population: population[1]<1000000)

large_count = large_countries.count()
small_list = small_countries.collect()

print(large_count)
print(small_list)

## 6. DAGs and Lazy Evaluation ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")
header = population_rdd.first()
clean_data = population_rdd.filter(lambda line: line != header)
split_data = clean_data.map(lambda line: line.split(','))
country_population = split_data.map(lambda fields: (fields[0], int(fields[1])))

# Build a complex transformation pipeline
large_population_pipeline = country_population  \
.filter(lambda population_in_millions: population_in_millions[1]>100000000) \
.map(lambda column: (column[0],column[1]/1_000_000)) \
.filter(lambda population_in_millions: population_in_millions[1]>200)

print(large_population_pipeline)
final_count = large_population_pipeline.count()
final_results = large_population_pipeline.collect()
print(final_count)
print(final_results)

## 7. Aggregating Data ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")
header = population_rdd.first()
clean_data = population_rdd.filter(lambda line: line != header)
split_data = clean_data.map(lambda line: line.split(','))
country_population = split_data.map(lambda fields: (fields[0], int(fields[1])))

# Calculate global population statistics

smallest_country = country_population.reduce(lambda x,y: x if x[1]<y[1] else y)
largest_country = country_population.reduce(lambda x,y: x if x[1]>y[1] else y)

populations = country_population.map(lambda population:population[1])
total_population = populations.reduce(lambda x,y: x+y)
avg_population = total_population/populations.count()

print(total_population)
print(avg_population)

## 8. Key-Value Operations ##

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("population-analysis").getOrCreate()
population_rdd = spark.sparkContext.textFile("world_population.csv")
header = population_rdd.first()
clean_data = population_rdd.filter(lambda line: line != header)
split_data = clean_data.map(lambda line: line.split(','))
country_population = split_data.map(lambda fields: (fields[0], int(fields[1])))

# Analyze population distribution by categories

def categorize_population(population):
    if population>=100000000:
        return 'Very Large'
    if population>=10000000:
        return 'Large'
    if population>=1000000:
        return 'Medium'
    else:
        return 'Small'

population_categories = country_population \
.map(lambda population: (categorize_population(population[1]),1))

category_counts = population_categories.reduceByKey(lambda x,y: x+y)
results = category_counts.collect()
print(results)
    
    