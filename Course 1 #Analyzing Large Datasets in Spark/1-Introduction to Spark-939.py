## 2. What are Apache Spark and PySpark? ##

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("DailyShowData") \
.getOrCreate()

daily_show_rdd = spark.sparkContext.textFile("daily_show.tsv")

preview_data = daily_show_rdd.take(5)

print(preview_data)

## 3. The Evolution of Spark Entry Points ##

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder \
.appName("ContextComparison") \
.getOrCreate()

context_from_session = spark.sparkContext
context_direct = SparkContext.getOrCreate()

print(context_from_session)
print(context_direct)

same_object = context_from_session is context_direct
print(same_object)

## 4. PySpark Architecture: Driver ##

from pyspark.sql import SparkSession

# Create a SparkSession with custom configuration
spark = (SparkSession.builder
    .appName("DriverAnalysis")
    .master("local[4]")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

app_name = spark.sparkContext.appName
master_config = spark.sparkContext.master
web_ui_url = spark.sparkContext.uiWebUrl

# Access Driver information
print(app_name)
print(master_config)
print(web_ui_url)

## 5. PySpark Architecture: Executors ##

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("ExecutorTest") \
.getOrCreate()

# This creates a plan but doesn't trigger Executors yet
daily_show_rdd = spark.sparkContext.textFile("daily_show.tsv")
print("RDD created:", daily_show_rdd)

parallelism_level = spark.sparkContext.defaultParallelism
# Now let's wake up the Executors with an action
sample_lines = daily_show_rdd.take(4)
print(parallelism_level)
print("Sample data:", sample_lines)

## 6. PySpark Architecture: Cluster ##

from pyspark.sql import SparkSession

# Configure a local cluster simulation
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("LocalCluster") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

cluster_mode = spark.sparkContext.master
executor_mem = spark.conf.get("spark.executor.memory")
driver_mem = spark.conf.get("spark.driver.memory")

print("Cluster mode:", cluster_mode)
#print("Application name:", spark.sparkContext.appName)  
print("Executor memory:", executor_mem)
print("Driver memory:", driver_mem)

spark.stop()

## 7. Lazy Evaluation and Actions ##

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("LazyTest") \
.getOrCreate()

guest_rdd = spark.sparkContext.textFile("daily_show.tsv")
print("Plan created:", guest_rdd)

total_lines = guest_rdd.count()
print("Total number of lines: ", total_lines)

header_line = guest_rdd.first()
print("First line: ", header_line)