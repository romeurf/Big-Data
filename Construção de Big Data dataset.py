from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, avg, sum as spark_sum
from pyspark.sql.types import StringType
import time
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Romeu\AppData\Local\Microsoft\WindowsApps\python.exe"  # Adjust to your Python installation
os.environ["JAVA_HOME"] = r"C:\java\jdk-24_windows-x64_bin.exe"  # Adjust based on your Java installation
os.environ["SPARK_HOME"] = r"/opt/spark-3.1.2-bin-hadoop3.2"  # Adjust based on your Spark installation

# =============================================================================
# 1. Initialize Spark Session and measure start time
# =============================================================================
spark = SparkSession.builder \
    .appName("Test") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

start_time = time.time()

# =============================================================================
# 2. Load DataFrames from CSV files
# =============================================================================
df_suicide = spark.read.csv('suicide_rate_by_country_2024.csv', header=True, inferSchema=True)
df_social  = spark.read.csv('Social_Progress_Index_2022.csv', header=True, inferSchema=True)
df_country = spark.read.csv('country_data.csv', header=True, inferSchema=True)
df_WHR     = spark.read.csv('WHR2024.csv', header=True, inferSchema=True)

# =============================================================================
# 3. Standardize Country Names with a UDF
# =============================================================================
country_mapping = {
    'United States': 'United States of America',
    'United States Virgin Islands': 'United States of America',
    'USA': 'United States of America',
    'US': 'United States of America',
    'United Kingdom': 'United Kingdom',
    'UK': 'United Kingdom',
    'Great Britain': 'United Kingdom',
    'Brunei Darussalam': 'Brunei',
    'Bolivia, Plurinational State Of': 'Bolivia',
    'Cabo Verde': 'Cape Verde',
    'Congo (Brazzaville)': 'Republic of the Congo',
    'Congo (Kinshasa)': 'Democratic Republic of the Congo',
    'Dr Congo': 'Democratic Republic of the Congo',
    'Congo': 'Democratic Republic of the Congo',
    'Congo, Democratic Republic Of': 'Democratic Republic of the Congo',
    'Congo, Republic Of': 'Republic of the Congo',
    'Congo, The Democratic Republic Of The': 'Democratic Republic of the Congo',
    'Korea, Republic of': 'South Korea',
    "Korea, Democratic People's Republic Of": 'North Korea',
    'Russian Federation': 'Russia',
    'Ivory Coast': "Côte d'Ivoire",
    "Cote D'Ivoire": "Côte d'Ivoire",
    'Czech Republic': 'Czechia',
    'Moldova, Republic Of': 'Moldova',
    'Micronesia, Federated States Of': 'Micronesia',
    'Macedonia, The Former Yugoslav Republic Of': 'Macedonia',
    'North Macedonia': 'Macedonia',
    "Lao People's Democratic Republic": 'Laos',
    'Republic Of North Macedonia': 'Macedonia',
    'Viet Nam': 'Vietnam',
    'Turkey': 'Türkiye',
    'Turkiye': 'Türkiye',
    'Iran, Islamic Republic Of': 'Iran',
    'Syria': 'Syrian Arab Republic',
    'Gambia, The': 'The Gambia',
    'Swaziland': 'Eswatini',
    'Palestine': 'State of Palestine',
    'Taiwan Province of China': 'Taiwan',
    'Tanzania, United Republic Of': 'Tanzania',
    'Hong Kong S.A.R. Of China': 'Hong Kong',
    'Venezuela, Bolivarian Republic Of': 'Venezuela'
}

def standardize_country(name):
    if name is None:
        return None
    name = name.strip()
    return country_mapping.get(name, name)

standardize_country_udf = udf(standardize_country, StringType())

# =============================================================================
# 4. Rename and Standardize Country Columns for Each DataFrame
# =============================================================================
# For WHR, assume country column is "Country name"
df_WHR = df_WHR.withColumnRenamed("Country name", "country") \
               .withColumn("country", standardize_country_udf(col("country")))

# For country data, assume country column is "name"
df_country = df_country.withColumnRenamed("name", "country") \
                       .withColumn("country", standardize_country_udf(col("country")))

# For social data, assume country column is "Country"
df_social = df_social.withColumnRenamed("Country", "country") \
                     .withColumn("country", standardize_country_udf(col("country")))

# For suicide data, assume the country column is already named "country"
df_suicide = df_suicide.withColumn("country", standardize_country_udf(col("country")))

# =============================================================================
# 5. Merge DataFrames on the "country" Column using Outer Joins
# =============================================================================
merged_df = df_WHR.join(df_country, on="country", how="outer") \
                  .join(df_social, on="country", how="outer") \
                  .join(df_suicide, on="country", how="outer")

# =============================================================================
# 6. Clean Dataset: Drop Rows with >50% Missing Values and Fill Missing Numeric Columns
# =============================================================================
# Calculate total number of columns
total_cols = len(merged_df.columns)

# Create a column counting non-null values in each row (excluding the helper if any)
non_null_expr = sum(when(col(c).isNotNull(), 1).otherwise(0) for c in merged_df.columns)
merged_df = merged_df.withColumn("non_null_count", non_null_expr)

# Keep rows with at least half non-null values
merged_df = merged_df.filter(col("non_null_count") >= (total_cols / 2)).drop("non_null_count")

# Identify numeric columns in the DataFrame (based on schema)
numeric_cols = [field.name for field in merged_df.schema.fields if str(field.dataType) in ['IntegerType', 'DoubleType', 'LongType', 'FloatType']]

# Compute average for each numeric column and fill missing values using those averages
avg_dict = {}
for col_name in numeric_cols:
    avg_val = merged_df.select(avg(col(col_name))).first()[0]
    if avg_val is not None:
        avg_dict[col_name] = avg_val

merged_df = merged_df.na.fill(avg_dict)

# =============================================================================
# 7. Write the Final Merged Dataset to Disk (CSV or Parquet)
# =============================================================================
output_path = "merged_dataset_spark.csv"
merged_df.write.csv(output_path, header=True, mode="overwrite")
# Alternatively, use Parquet:
# merged_df.write.parquet("merged_dataset_spark.parquet", mode="overwrite")

# Trigger an action to materialize the transformations and measure execution time
_ = merged_df.count()

end_time = time.time()
print("Tempo de execução com PySpark: {:.2f} segundos".format(end_time - start_time))

spark.stop()