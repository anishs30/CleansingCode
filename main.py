from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName('Spark example').getOrCreate()

# Checking null values
# has_nulls = patient_df.dropna().count() < patient_df.count()
# if has_nulls:
#     print("The dataset has null values.")
# else:
#     print("The dataset does not have null values.")



# Patient_record
patient_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/Patient_records.csv")

patient_df.select([count(when(col(c).isNull(), c)).alias(c) for c in patient_df.columns])
patient_df = patient_df.fillna('NA', subset=['patient_name', 'patient_phone'])

patient_has_duplicates = patient_df.dropDuplicates().count() < patient_df.count()
if patient_has_duplicates:
    patient_no_duplicates = patient_df.dropDuplicates()
else:
    patient_no_duplicates = patient_df
patient_no_duplicates.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east"
                                                             "-1.redshift-serverless.amazonaws.com:5439/dev").option(
    "dbtable", "Project_clean.patient").option("aws_iam_role",
                                               "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver",
                                                                                                      "com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()



# Disease
disease_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/disease.csv")

disease_has_duplicates = disease_df.dropDuplicates().count() < disease_df.count()

if disease_has_duplicates:
    disease_no_duplicates = disease_df.dropDuplicates()

else:
    disease_no_duplicates = disease_df

disease_final = disease_no_duplicates.withColumnRenamed(" Disease_ID", "Disease_Id")
disease_final.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1"
                                                     ".redshift-serverless.amazonaws.com:5439/dev").option("dbtable",
                                                                                                           "Project_clean.disease").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()



#subscribers
sub_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("s3://datatakeo/capstoneProject/subscriber.csv")

sub_null_counts = sub_df.select([count(when(col(c).isNull(), c)).alias(c) for c in sub_df.columns])
sub_df = sub_df.fillna('NA', subset=['first_name', 'phone', 'Subgrp_id', 'Elig_ind'])
sub_has_duplicates = sub_df.dropDuplicates().count() < sub_df.count()

if sub_has_duplicates:
    sub_final = sub_df.dropDuplicates()
    print("Duplicates")
else:
    sub_final = sub_df
    print("No duplicates")
sub_final = sub_final.withColumnRenamed("sub _id", "sub_id").withColumnRenamed("Zip Code", "zip_code")

sub_final.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift"
                                                 "-serverless.amazonaws.com:5439/dev").option("dbtable",
                                                                                              "Project_clean"
                                                                                              ".subscribers").option(
    "aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver",
                                                                           "com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()




# claim
claim_df = spark.read.json("s3://datatakeo/capstoneProject/claims.json")

claim_has_duplicates = claim_df.dropDuplicates().count() < claim_df.count()
if claim_has_duplicates:
    claim_final = claim_df.dropDuplicates()

else:
    claim_final = claim_df
claim_final.write.format("redshift").option("url",
                                            "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option(
    "dbtable", "Project_clean.claim").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option(
    "driver", "com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option(
    "user", "admin").option("password", "Asus365675%").mode("overwrite").save()





#hospital
hospital_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/hospital.csv")
has_nulls = hospital_df.dropna().count() < hospital_df.count()

hospital_has_duplicates = hospital_df.dropDuplicates().count() < hospital_df.count()

if hospital_has_duplicates:
    hospital_no_duplicates = hospital_df.dropDuplicates()

else:
    hospital_no_duplicates = hospital_df
hospital_no_duplicates.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east"
                                                              "-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.hospital").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()



# groups
group_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/group.csv")
group_has_duplicates = group_df.dropDuplicates().count() < group_df.count()
if group_has_duplicates:
    group_no_duplicates = group_df.dropDuplicates()

else:
    group_no_duplicates = group_df

groupsub_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/grpsubgrp.csv")

groupsub_has_duplicates = groupsub_df.dropDuplicates().count() < groupsub_df.count()
if groupsub_has_duplicates:
    groupsub_no_duplicates = groupsub_df.dropDuplicates()

else:
    groupsub_no_duplicates = groupsub_df

group_final = group_no_duplicates.join(groupsub_no_duplicates, ["Grp_Id"])
group_final.write.format("redshift").option("url",
                                            "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option(
    "dbtable", "Project_clean.group").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option(
    "driver", "com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option(
    "user", "admin").option("password", "Asus365675%").mode("overwrite").save()




# subgroups
sgroup_df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("s3://datatakeo/capstoneProject/subgroup.csv")

sgroup_has_duplicates = sgroup_df.dropDuplicates().count() < sgroup_df.count()
if group_has_duplicates:
    sgroup_no_duplicates = sgroup_df.dropDuplicates()

else:
    sgroup_no_duplicates = sgroup_df

sgroup_no_duplicates.write.format("redshift").option("url",
                                                     "jdbc:redshift://default-workgroup.556646946508.us-east-1"
                                                     ".redshift-serverless.amazonaws.com:5439/dev").option(
    "dbtable", "Project_clean.subgroup").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option(
    "driver", "com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option(
    "user", "admin").option("password", "Asus365675%").mode("overwrite").save()

# grouppsubgroup