from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName('Spark final').getOrCreate()

claims = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.claim").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
claims.createOrReplaceTempView("claim")

sub = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.subscribers").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
sub.createOrReplaceTempView("subscribers")

group = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.group").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
group.createOrReplaceTempView("groups")

patient = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.patient").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
patient.createOrReplaceTempView("patient")

hospital = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.hospital").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
hospital.createOrReplaceTempView("hospital")

subgroup = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.subgroup").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
subgroup.createOrReplaceTempView("subgroup")

disease = spark.read.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_clean.subgroup").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("tempdir", "s3a://datatakeo/tempRedShiftOutput/").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Asus365675%").load()
subgroup.createOrReplaceTempView("disease")

cityClaims = spark.sql("SELECT city, claim_count FROM (SELECT s.City AS city, COUNT(*) AS claim_count,RANK() OVER (ORDER BY COUNT(*) DESC) AS claim_rank FROM claim c JOIN subscribers s ON c.SUB_ID = s.sub_id GROUP BY s.City) ranked WHERE claim_rank = 1 ")
cityClaims.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Most_claims_city").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

cashless = spark.sql("SELECT s.first_name, s.last_name FROM subscribers s JOIN claim c ON s.sub_id = c.sub_id WHERE s.Elig_ind = 'Y' AND c.claim_amount >= 50000")
cashless.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Cashless_ChargesFiftyK").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

policies = spark.sql("SELECT g.Grp_Type, COUNT(s.Sub_ID) AS SubscriberCount FROM groups g LEFT JOIN subscribers s ON  g.Subgrp_id = s.Subgrp_id GROUP BY g.Grp_Type")
policies.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Subs_Gov_or_Private").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()
avgMonth = spark.sql("SELECT ROUND(AVG(sg.Monthly_Premium), 2) AS AveragePremium FROM subgroup sg LEFT JOIN subscribers s ON sg.Subgrp_id = s.Subgrp_id")
avgMonth.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Average_Month_Premium").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

female_knee = spark.sql("SELECT p.patient_name FROM patient p JOIN claim c ON p.patient_id = c.patient_id WHERE p.patient_gender = 'female' AND DATEDIFF(current_date(), p.patient_birth_date) > 40 * 365 AND c.disease_name = 'knee surgery' AND c.claim_date >= DATE_SUB(current_date(), 365);")
female_knee.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Female_KneeSurgery").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

rejectedClaimsdf = spark.sql("SELECT COUNT(*) AS RejectedClaimsCount FROM claim WHERE CLaim_Or_Rejected == 'N' ")
rejectedClaimsdf.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.NumberOf_Rejected_claims").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

profit_Group = spark.sql("SELECT g.Grp_Name, SUM(g.premium_written - c.claim_amount) AS profit FROM groups g JOIN subscribers s ON g.subgrp_id = s.subgrp_Id JOIN claim c ON s.sub_id = c.SUB_ID WHERE claim_or_rejected = 'Y'  GROUP BY g.Grp_Name ORDER BY profit DESC LIMIT 1")
profit_Group.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Profitable_Group").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()


YoungCancer = spark.sql("SELECT patient_name FROM patient WHERE DATEDIFF(current_date(), patient_birth_date) < 18 * 365 AND disease_name = 'cancer'")
YoungCancer.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.CancerPatient_Less18").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()


subgroupSub = spark.sql("SELECT Subgrp_id, COUNT(*) AS subscriber_count FROM Subscribers WHERE Subgrp_id IS NOT NULL GROUP BY Subgrp_id ORDER BY subscriber_count DESC LIMIT 1;")
subgroupSub.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Subgroup_max_subscribe").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

patientHospital = patient.join(hospital, ["hospital_id"])
patientHospital.createOrReplaceTempView("patientHos")
hospitalPatient = spark.sql("Select hospital_name from patientHos group by hospital_name order by Count(hospital_name) DESC LIMIT 1 ")
hospitalPatient.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Hospital_max_patient").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()
maxSubgroup = spark.sql("SELECT Grp_ID, COUNT(*) AS subgroup_count FROM groups GROUP BY Grp_ID ORDER BY subgroup_count DESC LIMIT 1")
maxSubgroup.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Group_Max_Subgroup").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

maxClaim = spark.sql(" SELECT disease_name, COUNT(*) AS claim_count FROM Claim GROUP BY disease_name ORDER BY claim_count DESC LIMIT 1; ")
maxClaim.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Disease_Max_claims").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()

sub_age= spark.sql("SELECT sub_id, first_name, last_name FROM subscribers WHERE DATEDIFF(current_date(), birth_date) / 365 < 30 AND subgrp_id IS NOT NULL")
sub_age.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.556646946508.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "Project_output.Subscribers_Age_Less30").option("aws_iam_role", "arn:aws:iam::556646946508:role/redshiftAdmin").option("driver","com.amazon.redshift.jdbc42.Driver").option("tempdir", "s3a://datatakeo/tempRedShiftProjecct/").option("user", "admin").option("password", "Asus365675%").mode("overwrite").save()