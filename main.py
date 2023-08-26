from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName('Spark example').getOrCreate()



# Patient_record
patient_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/Patient_records.csv")

patient_df.select([count(when(col(c).isNull(), c)).alias(c) for c in patient_df.columns]).show()
patient_df.fillna('NA', subset=['patient_name', 'patient_phone']).show()

patient_has_duplicates = patient_df.dropDuplicates().count() < patient_df.count()
if patient_has_duplicates:
    patient_no_duplicates = patient_df.dropDuplicates()
else:
    patient_no_duplicates = patient_df




# Disease
disease_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/disease.csv")

disease_has_duplicates = disease_df.dropDuplicates().count() < disease_df.count()

if disease_has_duplicates:
    disease_no_duplicates = disease_df.dropDuplicates()

else:
    disease_no_duplicates = disease_df



#subscribers
sub_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("s3://datatakeo/capstoneProject/subscriber.csv")


sub_df.select([count(when(col(c).isNull(), c)).alias(c) for c in sub_df.columns]).show()
sub_df.fillna('NA', subset=['first_name', 'phone', 'Subgrp_id', 'Elig_ind']).show()
sub_has_duplicates = sub_df.dropDuplicates().count() < sub_df.count()

if sub_has_duplicates:
    sub_final = sub_df.dropDuplicates()
    print("Duplicates")
else:
    sub_final = sub_df
    print("No duplicates")


# claim
claim_df = spark.read.json("s3://datatakeo/capstoneProject/claims.json")

claim_has_duplicates = claim_df.dropDuplicates().count() < claim_df.count()
if claim_has_duplicates:
    claim_final = claim_df.dropDuplicates()

else:
    claim_final = claim_df

