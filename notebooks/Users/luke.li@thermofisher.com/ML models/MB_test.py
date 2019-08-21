# Databricks notebook source
master = sqlContext.read.format("csv").load("/mnt/tfs-corporate/VW_F_SLS_BASE_DLY.csv",header=True).select('PROD_KEY','CUST_KEY','CONTACT_KEY','DT_KEY','ORD_CHNL_GRP_CD','PLATFORM_NM','SUB_PLATFORM_NM','SHIP_QTY','EXT_SLS_PMAR_AMT','RGN_CD','ACCT_SGN_ID','CON_SGN_ID','SRC_SLS_ORD_NBR')

# COMMAND ----------

master.createOrReplaceTempView('master')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select min(DT_KEY),max(DT_KEY) from master

# COMMAND ----------

#import master table
master = sqlContext.read.format("csv").load("/mnt/tfs-corporate/VW_F_SLS_BASE_DLY.csv",header=True).select('PROD_KEY','CUST_KEY','CONTACT_KEY','DT_KEY','ORD_CHNL_GRP_CD','PLATFORM_NM','SUB_PLATFORM_NM','SHIP_QTY','EXT_SLS_PMAR_AMT','RGN_CD','ACCT_SGN_ID','CON_SGN_ID','SRC_SLS_ORD_NBR')
#import contact table
contact  = sqlContext.sql("select * from dim_contact_3ids").select('CONTACT_KEY','CONTACT_ID')


#import product table
#product = sqlContext.sql("select * from dim_prod").select('PROD_KEY','PROD_LINE_GRP_CD','PROD_HIER_KEY','PROD_LINE_CD','SKU_NBR','PROD_DESC','BUS_AREA_GRP_NM')
product = sqlContext.sql("select * from dim_prod").select('PROD_KEY','PROD_HIER_KEY','SKU_NBR','PROD_DESC')

#import prod hier table to join with product table
prod_hier = sqlContext.sql("select * from dim_prod_hier").select('PROD_HIER_KEY','SUB_PROD_LINE_CD','BUS_AREA_GRP_NM','PROD_LINE_GRP_NM','PROD_LINE_CD')

#import date table
date = sqlContext.sql('Select * from export_dim_dt_csv').select('DT_KEY','FSCL_YR_WK_NBR','FSCL_YR_QTR_NBR')

#import customer table
cust = sqlContext.sql("select * from dim_cust").select('CUST_KEY','CUST_ID')

#joins
prod = product.join(prod_hier,['PROD_HIER_KEY'],'inner')

master_join = master.join(contact,['CONTACT_KEY'],'inner')\
                    .join(prod, ["PROD_KEY"],'inner')\
                    .join(date, ['DT_KEY'],'inner')\
                    .join(cust, ['CUST_KEY'],'inner')
      


# COMMAND ----------

from pyspark.sql.functions import split
split_col = split(master_join['PROD_LINE_GRP_NM'], '-')
master_join = master_join.withColumn('PROD_LINE_GRP_CD',split_col.getItem(0))

master_join = master_join.select('RGN_CD','ORD_CHNL_GRP_CD','PROD_LINE_GRP_CD','PROD_LINE_CD','SUB_PROD_LINE_CD','SKU_NBR','PROD_DESC','PLATFORM_NM','SUB_PLATFORM_NM','BUS_AREA_GRP_NM','ACCT_SGN_ID','CONTACT_ID','CUST_ID','CON_SGN_ID','DT_KEY','FSCL_YR_WK_NBR','FSCL_YR_QTR_NBR','EXT_SLS_PMAR_AMT','SHIP_QTY','SRC_SLS_ORD_NBR')

from pyspark.sql.functions import *
master_join = master_join.withColumn('RGN_CD', regexp_replace('RGN_CD', 'US', 'NA')).withColumn('RGN_CD', regexp_replace('RGN_CD', 'CA', 'NA'))

from pyspark.sql.functions import col,sum,avg,count
master_join = master_join.filter(master_join.CONTACT_ID != '?').filter(master_join.CON_SGN_ID != '?').filter(master_join.DT_KEY >= 20120101).filter(col('PROD_LINE_GRP_CD').isin(['?','FRT', 'XM2','XG2','XC2','ZZZ']) == False)

master_join = master_join.groupBy('RGN_CD',"ORD_CHNL_GRP_CD","PROD_LINE_GRP_CD","PROD_LINE_CD","SUB_PROD_LINE_CD","SKU_NBR","PROD_DESC","PLATFORM_NM","SUB_PLATFORM_NM",'BUS_AREA_GRP_NM','ACCT_SGN_ID','CONTACT_ID','CON_SGN_ID','CUST_ID','DT_KEY','FSCL_YR_WK_NBR','FSCL_YR_QTR_NBR','SRC_SLS_ORD_NBR').agg(sum('EXT_SLS_PMAR_AMT'),sum("SHIP_QTY"),count("*"))

master_join = master_join.filter(master_join['sum(EXT_SLS_PMAR_AMT)'] > 0).filter(master_join['sum(SHIP_QTY)'] > 0)

from pyspark.sql.functions import *

master_join = master_join.withColumn("Instrument_flag",when(col("BUS_AREA_GRP_NM").like('%INST%'), 1).otherwise(0))

master_join = master_join.withColumnRenamed('sum(EXT_SLS_PMAR_AMT)','Revenue').withColumnRenamed('sum(SHIP_QTY)','QTY').withColumnRenamed('count(1)','TRANS_FREQ')

AG_2012_17_SKU_REV = master_join

# COMMAND ----------

AG_2012_17_SKU_REV.createOrReplaceTempView('AG_2012_17_SKU_REV')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select min(DT_KEY),max(DT_KEY) from AG_2012_17_SKU_REV

# COMMAND ----------

# MAGIC %md contact

# COMMAND ----------

AG_contact_sku_yearly = sqlContext.sql('''
select b.* from
(
select a.* 
,COUNT(a.contact_id) OVER (PARTITION BY PROD_LINE_GRP_CD,PROD_LINE_CD,SKU_NBR)  AS SKU_cnt
from 
(select 
con_sgn_id
,a.contact_id
,PROD_LINE_CD
,prod_line_grp_cd
,Instrument_flag
,SKU_NBR
,SUM(Revenue) Revenue
,SUM(Qty) Qty,
 DT_KEY
from AG_2012_17_SKU_REV   a
where DT_KEY between date_format(date_sub(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),607),"yyyyMMdd") and date_format(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),"yyyyMMdd")
group by 
con_sgn_id
,a.contact_id
,PROD_LINE_CD
,Instrument_flag
,prod_line_grp_cd
,SKU_NBR,DT_KEY) a ) b
where sku_cnt >=20 or (instrument_flag=1 and sku_cnt >= 10)''')

# COMMAND ----------

AG_contact_sku_yearly.createOrReplaceTempView('AG_contact_sku_yearly')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select min(DT_KEY),max(DT_KEY) from AG_contact_sku_yearly

# COMMAND ----------

# MAGIC %md ## Market basket - SKU level - Group Contact

# COMMAND ----------

from pyspark.sql import functions as sf
from pyspark.sql.functions import collect_list
sku_level =  AG_contact_sku_yearly.select('CONTACT_ID','SKU_NBR')\
                     .groupBy('CONTACT_ID').agg(collect_set('SKU_NBR'))

# COMMAND ----------

display(sku_level)

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
#fpGrowth = FPGrowth(itemsCol="collect_list(SKU_NBR)", minSupport=0.01, minConfidence=0.02)
fpGrowth = FPGrowth(itemsCol="collect_set(SKU_NBR)", minSupport=50/sku_level.count(), minConfidence=0.02)
basket_model_sku_contact = fpGrowth.fit(sku_level)


# COMMAND ----------

20/190935

# COMMAND ----------

sku_level.count()

# COMMAND ----------

sku_contact_freq = basket_model_sku_contact.freqItemsets
sku_contact_rules = basket_model_sku_contact.associationRules.sort('confidence',ascending=False).filter(size('antecedent')==1)

# COMMAND ----------

display(sku_contact_rules)

# COMMAND ----------

sku_contact_rules.count()

# COMMAND ----------

get_pl_sku = AG_contact_sku_yearly.groupBy('PROD_LINE_GRP_CD','PROD_LINE_CD','SKU_NBR').count().select('PROD_LINE_CD','SKU_NBR')

# COMMAND ----------

# calculate frequency of rule and freq of antecedent
sku_con_rules = sku_contact_rules.toPandas() 
sku_con_freq = sku_contact_freq.toPandas()
#convert list to string
sku_con_rules['concat1'] = sku_con_rules['antecedent'] + sku_con_rules['consequent']
sku_con_rules['concat2'] = sku_con_rules['consequent'] + sku_con_rules['antecedent']
sku_con_rules['string_ante'] = sku_con_rules['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_con_rules['concat1'] = sku_con_rules['concat1'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_con_rules['concat2'] = sku_con_rules['concat2'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_con_freq['contcat'] = sku_con_freq['items'].apply(lambda x: "".join(x).encode('UTF-8'))
#merge the dataframes
import pandas as pd
one = pd.merge(sku_con_rules,sku_con_freq,left_on='concat1',right_on='contcat')
two = pd.merge(sku_con_rules,sku_con_freq,left_on='concat2',right_on='contcat')
#reset index for consistence/filter columns
onetwo = pd.concat([one,two]).reset_index()
onetwo.drop('index',inplace=True,axis=1)
onetwo = onetwo[['antecedent','consequent','confidence','freq']]
onetwo.columns = [['antecedent','consequent','confidence','rule_freq']]
#merge with the freqeuncy of antecedent
onetwo['string_ante'] = onetwo['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_contact_final = pd.merge(onetwo,sku_con_freq,left_on = 'string_ante',right_on ='contcat')[['antecedent','consequent','confidence','rule_freq','freq']]
sku_contact_final.columns = [['antecedent','consequent','confidence','rule_freq','ante_freq']]
sku_contact_final['antecedent'] = sku_contact_final['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_contact_final['consequent'] = sku_contact_final['consequent'].apply(lambda x: "".join(x).encode('UTF-8'))
#display(sqlContext.createDataFrame(sku_contact_final))


# COMMAND ----------

sku_contact_final['antecedent'] = sku_contact_final['antecedent'].apply(lambda x: x.decode("utf-8"))
sku_contact_final['consequent'] = sku_contact_final['consequent'].apply(lambda x: x.decode("utf-8"))

# COMMAND ----------

temp1 = sqlContext.createDataFrame(sku_contact_final)
sub_temp1 = temp1.join(product,temp1.antecedent == product.SKU_NBR).select('antecedent','PROD_DESC','consequent','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','Ante_SKU_NAME')
sub_temp1 = sub_temp1.join(product,sub_temp1.consequent == product.SKU_NBR).select('antecedent','Ante_SKU_NAME','consequent','PROD_DESC','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','CONS_SKU_NAME')
contact_temp = sub_temp1.join(get_pl_sku,sub_temp1['antecedent'] == get_pl_sku['SKU_NBR'],'inner').drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','ANTE_PL')
contact_final = contact_temp.join(get_pl_sku, contact_temp['consequent'] == get_pl_sku['SKU_NBR']).drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','CON_PL')
display(contact_final)

# COMMAND ----------

# MAGIC %md order

# COMMAND ----------

# ag_contact_sku_yearly for order grouping

AG_contact_sku_yearly_order = sqlContext.sql('''
select b.* from
(
select a.* 
,COUNT(a.SRC_SLS_ORD_NBR) OVER (PARTITION BY PROD_LINE_GRP_CD,PROD_LINE_CD,SKU_NBR)  AS SKU_cnt
from 
(select 
con_sgn_id
,a.SRC_SLS_ORD_NBR
,PROD_LINE_CD
,prod_line_grp_cd
,Instrument_flag
,SKU_NBR
,SUM(Revenue) Revenue
,SUM(Qty) Qty
,DT_KEY
from AG_2012_17_SKU_REV   a
where DT_KEY between date_format(date_sub(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),607),"yyyyMMdd") and date_format(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),"yyyyMMdd")
group by 
con_sgn_id
,a.SRC_SLS_ORD_NBR
,PROD_LINE_CD
,Instrument_flag
,prod_line_grp_cd
,SKU_NBR,
DT_KEY) a ) b
where sku_cnt >=20 or (instrument_flag=1 and sku_cnt >= 10)''')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select date_format(date_sub(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),365),"yyyyMMdd") , date_format(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),"yyyyMMdd")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(DT_KEY),max(DT_KEY) from ord1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select date_sub(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),365)

# COMMAND ----------

AG_contact_sku_yearly_order.createOrReplaceTempView('ord1')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select min(DT_KEY),max(DT_KEY) from ord

# COMMAND ----------

# MAGIC %md ## Market Basket - SKU - Group by Order Id

# COMMAND ----------

df_sku = AG_contact_sku_yearly_order.select('src_sls_ord_nbr','SKU_NBR').drop_duplicates().groupBy('src_sls_ord_nbr').agg(collect_set('SKU_NBR'))

# COMMAND ----------

minSup = 20/df_sku.count()

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
fpGrowth = FPGrowth(itemsCol="collect_set(SKU_NBR)", minSupport=minSup, minConfidence=0.02)
#fpGrowth = FPGrowth(itemsCol="collect_list(SKU_NBR)", minSupport=0.01, minConfidence=0.02)
basket_model_sku_order = fpGrowth.fit(df_sku)

# COMMAND ----------

sku_order_freq = basket_model_sku_order.freqItemsets

# COMMAND ----------

sku_order_rules = basket_model_sku_order.associationRules.sort('confidence',ascending=False).filter(size('antecedent')==1)

# COMMAND ----------

sku_order_rules.count()

# COMMAND ----------

# calculate frequency of rule and freq of antecedent
sku_ord_rules = sku_order_rules.toPandas() 
sku_ord_freq = sku_order_freq.toPandas()
#convert list to string
sku_ord_rules['concat1'] = sku_ord_rules['antecedent'] + sku_ord_rules['consequent']
sku_ord_rules['concat2'] = sku_ord_rules['consequent'] + sku_ord_rules['antecedent']
sku_ord_rules['string_ante'] = sku_ord_rules['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ord_rules['concat1'] = sku_ord_rules['concat1'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ord_rules['concat2'] = sku_ord_rules['concat2'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ord_freq['contcat'] = sku_ord_freq['items'].apply(lambda x: "".join(x).encode('UTF-8'))
#merge the dataframes
import pandas as pd
one = pd.merge(sku_ord_rules,sku_ord_freq,left_on='concat1',right_on='contcat')
two = pd.merge(sku_ord_rules,sku_ord_freq,left_on='concat2',right_on='contcat')
#reset index for consistence/filter columns
onetwo = pd.concat([one,two]).reset_index()
onetwo.drop('index',inplace=True,axis=1)
onetwo = onetwo[['antecedent','consequent','confidence','freq']]
onetwo.columns = [['antecedent','consequent','confidence','rule_freq']]
#merge with the freqeuncy of antecedent
onetwo['string_ante'] = onetwo['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ord_final = pd.merge(onetwo,sku_ord_freq,left_on = 'string_ante',right_on ='contcat')[['antecedent','consequent','confidence','rule_freq','freq']]
sku_ord_final.columns = [['antecedent','consequent','confidence','rule_freq','ante_freq']]
sku_ord_final['antecedent'] = sku_ord_final['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ord_final['consequent'] = sku_ord_final['consequent'].apply(lambda x: "".join(x).encode('UTF-8'))
#display(sqlContext.createDataFrame(sku_ord_final))

sku_ord_final['antecedent'] = sku_ord_final['antecedent'].apply(lambda x: x.decode("utf-8"))
sku_ord_final['consequent'] = sku_ord_final['consequent'].apply(lambda x: x.decode("utf-8"))

temp2 = sqlContext.createDataFrame(sku_ord_final)
sub_temp2 = temp2.join(product,temp2.antecedent == product.SKU_NBR).select('antecedent','PROD_DESC','consequent','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','Ante_SKU_NAME')
sub_temp2 = sub_temp2.join(product,sub_temp2.consequent == product.SKU_NBR).select('antecedent','Ante_SKU_NAME','consequent','PROD_DESC','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','CONS_SKU_NAME')
order_temp = sub_temp2.join(get_pl_sku,sub_temp2['antecedent'] == get_pl_sku['SKU_NBR'],'inner').drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','ANTE_PL')
order_final = order_temp.join(get_pl_sku, order_temp['consequent'] == get_pl_sku['SKU_NBR']).drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','CON_PL')
display(order_final)

# COMMAND ----------

sku_ord_final

# COMMAND ----------

# MAGIC %md ## shipto grouping

# COMMAND ----------

# ag_cust_sky_yearly by shipto grouping

import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import functions as sf
sku_cust_sgn = master_join.where((master_join['DT_KEY'] > (datetime.today() - timedelta(days=365)).strftime('%Y%m%d')) & (master_join['DT_KEY'] < datetime.today().strftime('%Y%m%d')))\
                         .withColumn('comb',sf.concat(sf.col('CUST_ID'),sf.col('CON_SGN_ID')))
  
sku_cust_sgn = sku_cust_sgn.groupBy('PROD_LINE_GRP_CD','PROD_LINE_CD','comb','Instrument_flag','SKU_NBR')\
                         .agg(sum('Revenue'),sum('QTY'))\
                         .withColumn('SKU_cnt', (func.count(sku_cust_sgn['comb']).over(Window.partitionBy('PROD_LINE_GRP_CD','PROD_LINE_CD','SKU_NBR'))))
AG_cust_sku_yearly = sku_cust_sgn.where((sku_cust_sgn['SKU_cnt'] >= 50) | ((sku_cust_sgn['Instrument_flag'] == 1) & (sku_cust_sgn['SKU_cnt'] >= 20)))
AG_cust_sku_yearly = AG_cust_sku_yearly.withColumnRenamed('sum(Revenue)','Revenue').withColumnRenamed('sum(QTY)','QTY')

# COMMAND ----------

shipto_yearly = AG_cust_sku_yearly.select('comb','SKU_NBR').drop_duplicates().groupBy('comb').agg(collect_list('SKU_NBR'))

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth
fpGrowth = FPGrowth(itemsCol="collect_list(SKU_NBR)", minSupport=0.01, minConfidence=0.02)
basket_model_sku_shipto = fpGrowth.fit(shipto_yearly)

# COMMAND ----------

shipto_yearly.count()

# COMMAND ----------

91237 * 0.001

# COMMAND ----------

sku_shipto_freq = basket_model_sku_shipto.freqItemsets

# COMMAND ----------

sku_shipto_rules = basket_model_sku_shipto.associationRules.sort('confidence',ascending=False).filter(size('antecedent')==1)

# COMMAND ----------

sku_shipto_rules.count()

# COMMAND ----------

# calculate frequency of rule and freq of antecedent
sku_ship_rules = sku_shipto_rules.toPandas() 
sku_ship_freq = sku_shipto_freq.toPandas()
#convert list to string
sku_ship_rules['concat1'] = sku_ship_rules['antecedent'] + sku_ship_rules['consequent']
sku_ship_rules['concat2'] = sku_ship_rules['consequent'] + sku_ship_rules['antecedent']
sku_ship_rules['string_ante'] = sku_ship_rules['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ship_rules['concat1'] = sku_ship_rules['concat1'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ship_rules['concat2'] = sku_ship_rules['concat2'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_ship_freq['contcat'] = sku_ship_freq['items'].apply(lambda x: "".join(x).encode('UTF-8'))
#merge the dataframes
import pandas as pd
one = pd.merge(sku_ship_rules,sku_ship_freq,left_on='concat1',right_on='contcat')
two = pd.merge(sku_ship_rules,sku_ship_freq,left_on='concat2',right_on='contcat')
#reset index for consistence/filter columns
onetwo = pd.concat([one,two]).reset_index()
onetwo.drop('index',inplace=True,axis=1)
onetwo = onetwo[['antecedent','consequent','confidence','freq']]
onetwo.columns = [['antecedent','consequent','confidence','rule_freq']]
#merge with the freqeuncy of antecedent
onetwo['string_ante'] = onetwo['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_shipto_final = pd.merge(onetwo,sku_ship_freq,left_on = 'string_ante',right_on ='contcat')[['antecedent','consequent','confidence','rule_freq','freq']]
sku_shipto_final.columns = [['antecedent','consequent','confidence','rule_freq','ante_freq']]
sku_shipto_final['antecedent'] = sku_shipto_final['antecedent'].apply(lambda x: "".join(x).encode('UTF-8'))
sku_shipto_final['consequent'] = sku_shipto_final['consequent'].apply(lambda x: "".join(x).encode('UTF-8'))

sku_shipto_final['antecedent'] = sku_shipto_final['antecedent'].apply(lambda x: x.decode("utf-8"))
sku_shipto_final['consequent'] = sku_shipto_final['consequent'].apply(lambda x: x.decode("utf-8"))

temp3 = sqlContext.createDataFrame(sku_shipto_final)
sub_temp3 = temp3.join(product,temp3.antecedent == product.SKU_NBR).select('antecedent','PROD_DESC','consequent','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','Ante_SKU_NAME')
sub_temp3 = sub_temp3.join(product,sub_temp3.consequent == product.SKU_NBR).select('antecedent','Ante_SKU_NAME','consequent','PROD_DESC','confidence','rule_freq','ante_freq').withColumnRenamed('PROD_DESC','CONS_SKU_NAME')
shipto_temp = sub_temp3.join(get_pl_sku,sub_temp3['antecedent'] == get_pl_sku['SKU_NBR'],'inner').drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','ANTE_PL')
shipto_final = shipto_temp.join(get_pl_sku, shipto_temp['consequent'] == get_pl_sku['SKU_NBR']).drop('SKU_NBR').withColumnRenamed('PROD_LINE_CD','CON_PL')
display(shipto_final)

# COMMAND ----------

shipto_final.createOrReplaceTempView('shipto_final')
order_final.createOrReplaceTempView('order_final')
contact_final.createOrReplaceTempView('contact_final')

# COMMAND ----------

display(shipto_final)

# COMMAND ----------

shipto_final = sqlContext.sql('''select *,'SHIPTO' as SRC from shipto_final ''')
order_final = sqlContext.sql('''select *,'ORDER' as SRC from order_final ''')
contact_final = sqlContext.sql('''select *,'CONTACT' as SRC from contact_final ''')

# COMMAND ----------

# MAGIC %md ## Final Table

# COMMAND ----------

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

display(unionAll(contact_final,order_final,shipto_final))

# COMMAND ----------

final = unionAll(contact_final,order_final,shipto_final)

# COMMAND ----------

final.createOrReplaceTempView('final')

# COMMAND ----------

display(final)

# COMMAND ----------

display(sqlContext.sql("""
select b.* from final b inner join
(select antecedent,consequent, max(confidence) as CONFIDENCE from final group by antecedent,consequent) a
on b.antecedent = a.antecedent AND b.consequent = a.consequent AND b.CONFIDENCE = a.CONFIDENCE"""))

# COMMAND ----------

final_data = sqlContext.sql("""
select b.* from final b inner join
(select antecedent,consequent, max(confidence) as CONFIDENCE from final group by antecedent,consequent) a
on b.antecedent = a.antecedent AND b.consequent = a.consequent AND b.CONFIDENCE = a.CONFIDENCE""")

# COMMAND ----------

final_data.count()