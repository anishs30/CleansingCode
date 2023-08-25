from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName('Spark example').getOrCreate()

patient_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/Patient_records.csv")
has_nulls = patient_df.dropna().count() < patient_df.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

patient_null_counts = patient_df.select([count(when(col(c).isNull(), c)).alias(c) for c in patient_df.columns]).show()
patient_df.fillna('NA', subset=['patient_name', 'patient_phone']).show()
patient_has_duplicates = patient_df.dropDuplicates().count() < patient_df.count()

# If there are duplicates, drop them
if patient_has_duplicates:
    patient_no_duplicates = patient_df.dropDuplicates()

else:
    patient_no_duplicates = patient_df