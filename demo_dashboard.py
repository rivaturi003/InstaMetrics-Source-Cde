# Databricks notebook source
s3_prefix = "s3a://"
s3_bucket = "maw_bucket"
checkpoint_base_dir = "dw/checkpoint"
table_base_dir = "dw/table"

# COMMAND ----------

table_name = "campaign"
temp_table_name = "tmp_"+table_name
table_path = s3_prefix + s3_bucket + "/" + table_base_dir + "/" + table_name

df = spark.readStream.format("delta").load(table_path)
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

query="""
with e AS(
select 
      e0.kafka_timestamp
    , window(e0.kafka_timestamp, "5 minutes") as window_time
    , ifnull(e0.campaign, 'n/a') as campaign
    , e0.product_effective_unit_price
from tmp_campaign e0 
where 1=1 
  and e0.kafka_timestamp >= current_timestamp() - INTERVAL 1 HOUR
)
select 
    --date_format(t.window_time.end, "MMM-dd HH:mm")  as event_time 
    t.window_time.start
 ,  t.campaign
 ,  t.product_effective_unit_price
 ,  t.items_quantity
from (
select 
    e.window_time
 , e.campaign    
  
  , sum(product_effective_unit_price) as product_effective_unit_price
  , count(*) as items_quantity
from e 
group by 
1,2
)t 
order by 
1,2
    -- date_format(t.window_time.end, "MMM-dd HH:mm")
"""

df0 = spark.sql(query)  # returns another streaming DF
display(df0)


# COMMAND ----------

table_name = "product_impression"
temp_table_name = "tmp_"+table_name
table_path = s3_prefix + s3_bucket + "/" + table_base_dir + "/" + table_name
print(table_path)
print(temp_table_name)
df = spark.readStream.format("delta").load(table_path)
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

query="""
with e AS(
select 
      e0.kafka_timestamp
    , window(e0.kafka_timestamp, "5 minutes") as window_time
    , ifnull(e0.campaign, 'n/a') as campaign
    , e0.product_effective_unit_price
from tmp_product_impression e0 
where 1=1 
  and e0.kafka_timestamp >= current_timestamp() - INTERVAL 1 HOUR
)
select 
    --date_format(t.window_time.end, "MMM-dd HH:mm")  as event_time 
    t.window_time.start
 ,  t.campaign
 ,  t.product_effective_unit_price
 ,  t.items_quantity
from (
select 
    e.window_time
  , e.campaign
  , sum(product_effective_unit_price) as product_effective_unit_price
  , count(*) as items_quantity
from e 
group by 
1,2
)t 
order by 
    t.window_time.start
  , t.campaign
    -- date_format(t.window_time.end, "MMM-dd HH:mm")
"""

df1 = spark.sql(query)  # returns another streaming DF
display(df1)


# COMMAND ----------

query="""
select 
    e.campaign
  , e.product_name
  , sum(product_effective_unit_price) as product_effective_unit_price
  , count(*) as items_quantity
from tmp_product_impression e 
where 1=1 
  and e.kafka_timestamp >= current_timestamp() - INTERVAL 1 DAY
group by 
1,2
"""

df2 = spark.sql(query)  # returns another streaming DF
# display(df2)

