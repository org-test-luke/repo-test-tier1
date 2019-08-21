# Databricks notebook source
# MAGIC %fs
# MAGIC 
# MAGIC ls /user/hive/warehouse/

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.test5_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES  default.adult;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE default.adult;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cdwcm.d_asset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN cdwcm;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED oracle.zz_test; 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED  default.d_addr

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED omni.omni_two

# COMMAND ----------

import pandas as pd
df = pd.DataFrame({'x': [1, 2], 'y': [3, 4], 'z': [5, 6]})
# Rename columns
df.columns = ['x', 'y', 'z1']
# Do some operations in place
df['x2'] = df.x * df.y
 

# COMMAND ----------

y=col.get_values()

# COMMAND ----------

y.tolist()