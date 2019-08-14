# Databricks notebook source
# MAGIC %fs mounts

# COMMAND ----------

#BigD Acct S3 Bucket "bigd-iam-test"
#User luke.li 
ACCESS_KEY = 
SECRET_KEY = 

# The below steps are the same: 
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "bigd-iam-test"
MOUNT_NAME = "bigd-iam-test"

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# Check the Mount Point
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/bigd-iam-test/test_folder/")
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

df_cust = spark.read.text("/mnt/bigd-iam-test/test_folder/cust_list.csv") 
display (df_cust)

# COMMAND ----------

dbutils.fs.mount(
   "s3a://bigd-iam-test/sensitive",
   "/mnt/bigd-iam-test-Senstive",
   extra_configs = {
      "fs.s3a.credentialsType": "Custom",
      "fs.s3a.credentialsType.customClass": "com.databricks.backend.daemon.driver.aws.AwsCredentialContextTokenProvider",
      "fs.s3a.stsAssumeRole.arn": "arn:aws:iam::273212400438:role/TFS-Databricks-EC2Role-BIDL-DETier1"
})

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/tables/trend/trend_linux_prod.txt    dbfs:/databricks/init/trend/trend_linux_prod.txt

# COMMAND ----------

# MAGIC %fs cp  'file:\C:\LifeTech_Data\LifeTech_EDW_Data\AWS_Cloud\AWS_Accts\TrendAgent\trend_install_scripts\trend_linux_prod.txt'    ' dbfs:/databricks/init/trend/trend_linux_prod.txt'

# COMMAND ----------

dbutils.fs.rm("dbfs:/databricks/init/trend", True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks/init/testwz/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks/rstudio/

# COMMAND ----------

# MAGIC %fs rm dbfs:/databricks/init/trend_linux_prod.txt

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/tables/trend/trend_linux_prod.txt    dbfs:/databricks/init/trend_linux_prod.txt

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# DBTITLE 1,IAM Passthrough Test
dbutils.credentials.showRoles() 

# COMMAND ----------

#Print out Roles 
for role in dbutils.credentials.showRoles():
  print(role)

# COMMAND ----------

dbutils.credentials.showCurrentRole()

# COMMAND ----------

dbutils.credentials.assumeRole( "arn:aws:iam::273212400438:role/ADFS-Databricks-BIDL-DE-Tier1" )

# COMMAND ----------

dbutils.credentials.showCurrentRole()

# COMMAND ----------

# DBTITLE 1,Mount the S3 Bucket Without Any Credential Info 
# MAGIC %md
# MAGIC 
# MAGIC ### check  Mount Points
# MAGIC Checking S3 Mount Points
# MAGIC To List all the mount points:
# MAGIC 
# MAGIC display(dbutils.fs.ls("/mnt/"))
# MAGIC To Check each mount point's S3 Location
# MAGIC 
# MAGIC display(dbutils.fs.mounts())
# MAGIC 
# MAGIC ### Without Credential
# MAGIC dbutils.fs.mount(“s3a://buckectName”, “/mnt/pathName")
# MAGIC 
# MAGIC 
# MAGIC ### With Credential
# MAGIC 
# MAGIC ACCESS_KEY = "aws-access-key"
# MAGIC   
# MAGIC SECRET_KEY = "aws-secret-key"
# MAGIC   
# MAGIC ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
# MAGIC   
# MAGIC AWS_BUCKET_NAME = "aws-bucket-name"
# MAGIC   
# MAGIC MOUNT_NAME = "mount-name"
# MAGIC   
# MAGIC 
# MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
# MAGIC   
# MAGIC display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))
# MAGIC 
# MAGIC ### Test Access
# MAGIC 
# MAGIC Python: 
# MAGIC 
# MAGIC df = spark.read.text("/mnt/%s/...." % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#Can mount either in Standard or HC Cluster
dbutils.fs.mount("s3a://luke-test-databricks-iam",  "/mnt/bigds3-luke-test")

# COMMAND ----------

# DBTITLE 1,Untitled
dbutils.fs.ls("/mnt/bigds3-luke-test")

# COMMAND ----------

dbutils.fs.ls("/mnt/bigds3-luke-test/bids-de-tier1")

# COMMAND ----------

#dbutils.credentials.showRoles()
#dbutils.credentials.showCurrentRole()
#dbutils.credentials.assumeRole("arn:aws:iam::273212400438:role/ADFS-Databricks-BIDL-DE-Tier1") 
#dbutils.credentials.assumeRole("arn:aws:iam::273212400438:role/ADFS-Databricks-BIDS-DE-Tier1") 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Test Result:
# MAGIC 
# MAGIC #### Standard Cluster
# MAGIC 
# MAGIC Can't show up or assume any role. Access Denied 
# MAGIC 
# MAGIC #### HC Cluster
# MAGIC 
# MAGIC If not Assume Role: 