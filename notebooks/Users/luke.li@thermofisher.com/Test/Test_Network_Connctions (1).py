# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Test the Conneciton to Corp Data Center 
# MAGIC 
# MAGIC ## A. Ping the Exadata Prod Server Host and IP 
# MAGIC 
# MAGIC Exadata Prod Server: 
# MAGIC 
# MAGIC nslookup x03p-scan.amer.thermo.com   ==>  10.0.109.29,    10.0.109.30,    10.0.109.31

# COMMAND ----------

# MAGIC %sh
# MAGIC # Ping the Exadata Prod IP
# MAGIC ping 10.0.109.29

# COMMAND ----------

# MAGIC %sh
# MAGIC # Ping the Exadata Prod Host Name
# MAGIC ping x03p-scan.amer.thermo.com

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Check the Oracle port 1521 on Exadata Prod Server

# COMMAND ----------

# MAGIC %sh
# MAGIC # Check the Exadata Prod IP  port 1521 
# MAGIC telnet 10.0.109.29  1521

# COMMAND ----------

# MAGIC %md
# MAGIC # Test the Connection to BI-General-SS VPC
# MAGIC 
# MAGIC Notes: BI-General-SS VPC ahs two CIDR blocks: 
# MAGIC     + 10.244.154.0/25 
# MAGIC     + 10.244.70.128/25  
# MAGIC     
# MAGIC ## A.  Ping the Running EC2 Instances in BI-General-SS VPC (10.244.70.128/25)

# COMMAND ----------

# MAGIC %sh
# MAGIC # Ping a Running EC2 Instances in BI-General-SS VPC 
# MAGIC ping 10.244.70.156

# COMMAND ----------

# MAGIC %sh
# MAGIC #Cognos ContentStore Oracle Server on AWS Bi-General-SS:
# MAGIC #awsu1-25lcgd01p.amer.thermo.com:1521    10.244.70.200 
# MAGIC #awsu1-25lcgd01t.amer.thermo.com:1521    10.244.70.217
# MAGIC 
# MAGIC ping 10.244.70.200

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Ping the Running Redshift Lead Instances in BI-General-SS VPC (10.244.154.0/25

# COMMAND ----------

# MAGIC %sh
# MAGIC #Check the port connection to Redshift Clusters
# MAGIC 
# MAGIC #ping 10.244.154.58   redshift can't be pinged
# MAGIC telnet 10.244.154.58   5439
# MAGIC #telnet cdwtst.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com   5439

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Use Dig Command Examples To Query DNS
# MAGIC 
# MAGIC ### 1. Basic DNS Query
# MAGIC dig google.com
# MAGIC 
# MAGIC ### 2. Query Specific Name Server
# MAGIC dig @8.8.8.8 google.com
# MAGIC 
# MAGIC ### 3. Reverse DNS Lookup
# MAGIC dig -x 216.58.220.110

# COMMAND ----------

# MAGIC %sh
# MAGIC dig www.google.com

# COMMAND ----------

# MAGIC %sh
# MAGIC dig @8.8.8.8 google.com

# COMMAND ----------

# MAGIC %sh
# MAGIC dig -x 216.58.220.110

# COMMAND ----------

# MAGIC %sh
# MAGIC dig -x 10.244.70.156

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC #Test The Firewall for On-Permise Databases
# MAGIC 
# MAGIC 
# MAGIC 1. Exadata Prod:
# MAGIC x03p-scan.amer.thermo.com / 10.0.109.29-31
# MAGIC Port:  1521
# MAGIC 
# MAGIC 
# MAGIC 2. Exadata Dev and Test:
# MAGIC x03s-scan.amer.thermo.com / 10.0.108.31-33
# MAGIC Port:  1521
# MAGIC 
# MAGIC 3. E1 Report-Replica - E1V9IPY and E1V9ADG:
# MAGIC x04e1s-scan.amer.thermo.com / 10.0.108.24-26
# MAGIC Port:  1521
# MAGIC 
# MAGIC 4. Axeda Prod:-- DRMPRD:
# MAGIC f1smpd.corp.life / 10.0.130.109
# MAGIC Port: 1527
# MAGIC 
# MAGIC 5. Axeda Staging -- DRMSTG
# MAGIC f2smpd.stage.life / 10.0.128.163
# MAGIC Port: 1530

# COMMAND ----------

# MAGIC %sh
# MAGIC # telnet 10.0.109.29 1521  --OK
# MAGIC # telnet 10.0.108.31 1521  --OK
# MAGIC # telnet 10.0.108.24 1521  --OK
# MAGIC # telnet 10.0.130.109 1527   --Not Work
# MAGIC # telnet 10.0.128.163 1530   --Not Work
# MAGIC 
# MAGIC # telnet 10.0.130.109 1521  
# MAGIC # telnet 10.0.128.163 1521   

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/
# MAGIC java.io.FileNotFoundException: No such file or directory: /tfsprod/0/user/hive/warehouse

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.test5_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES  default.adult;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh telnet 10.0.128.16 1521

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT 'luke.li@thermofisher.com' ON DATABASE default

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT `luke.li@thermofisher.com` ON DATABASE default

# COMMAND ----------

import logging

# COMMAND ----------

logging.info ("XNYZ")

# COMMAND ----------

# MAGIC %sh
# MAGIC ping 10.32.72.229

# COMMAND ----------

# MAGIC %sh
# MAGIC telnet 10.32.72.229  5432