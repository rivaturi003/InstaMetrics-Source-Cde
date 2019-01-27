

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import boto3
import json 

schemaRegistryAddr = "http://avro-schema-registry.aws.shave.io:8081"

# subscribe to topic 
df = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "maw_kafka_server:9092")\
  .option("subscribe", "maw_kafka_topic")\
  .option("failOnDataLoss", "true")\
  .option("startingOffsets", "earliest")\
  .load()

# COMMAND ----------

# init global vars 

data_file_extension = ".data" 
schema_file_extension = ".schema" 
json_file_extension = ".json"

clicks_event_name = "campaign_clicks"
product_impression_event_name = "campaign_impressions"

# init S3 client (boto)
s3_prefix = "s3a://"
s3_bucket = "maw_bucket"
s3_checkpoint_base_dir = "ddw/checkpoint"
s3_schema_base_dir = "ddw/schema"
s3_table_base_dir = "ddw/table"

# init s3 client 
s3_client = boto3.client(
  's3',
  aws_access_key_id=s3_access_key,
  aws_secret_access_key=s3_secret_key
)

# read schemas from S3 

# read clicks  schema from S3 json schema file 
s3_schema_file = clicks_event_name+".nested.schema.json"
s3_schema_path_file = s3_schema_base_dir + "/" + s3_schema_file

s3_schema_obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_schema_path_file)
df_schema_json = json.loads(s3_schema_obj['Body'].read().decode('utf-8'))

schema_clicks = StructType.fromJson(df_schema_json)

# read product_impression schema from S3 json schema file 
s3_schema_file = product_impression_event_name+".schema.json"
s3_schema_path_file = s3_schema_base_dir + "/" + s3_schema_file

s3_schema_obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_schema_path_file)
df_schema_json = json.loads(s3_schema_obj['Body'].read().decode('utf-8'))

schema_product_impression= StructType.fromJson(df_schema_json)

# COMMAND ----------

# df.printSchema()
kafka_event_name = clicks_event_name
stream_write_query_name = kafka_event_name + "_stream_write_query_name"

event_filter = "payload like '%\"eventName\":\"{kafka_event_name}\"%'"\
.format(kafka_event_name=kafka_event_name)

tmp_nested_event_table = "tmp_nested_" + kafka_event_name

nested_event_df = df.selectExpr(
                    "CAST(value AS STRING) as payload", 
                    "CAST(topic AS STRING) as kafka_topic", 
                    "CAST(partition AS STRING) as kafka_partition", 
                    "CAST(offset AS INTEGER) as kafka_offset", 
                    "CAST(timestamp AS timestamp) as kafka_timestamp")\
.where(event_filter)\
.select(from_json("payload", schema_clicks).alias("pl"), "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")

nested_event_df.createOrReplaceTempView(tmp_nested_event_table)

# COMMAND ----------

query = """
select 
    e.session_id
  , e.tracksuit_version
  , e.user_agent
  , e.event_name
  
  , e.items.campaign as campaign
  , e.items.effectiveUnitPrice as item_effective_unit_price


  , e.kafka_topic
  , e.kafka_partition
  , e.kafka_offset

  , cast(date_format(e.kafka_timestamp, "y-MM-dd HH:mm:ss.SSS") as timestamp) as kafka_timestamp 
  , cast(date_format(e.kafka_timestamp, "y-MM-dd") as date) as kafka_date
  , cast(year(e.kafka_timestamp) as integer) as yyyy
  , cast(month(e.kafka_timestamp) as integer) as mm
  , cast(day(e.kafka_timestamp) as integer) as dd 
  , cast(date_format(current_timestamp(), "y-MM-dd HH:mm:ss.SSS") as timestamp) as stream_load_timestamp
  
from (
  select 
      explode_outer(pl.data.data.data.items) as items
    , pl.data.meta.sessionId as session_id
    , kafka_topic
    , kafka_partition
    , kafka_offset
    , kafka_timestamp
  from {tmp_nested_event_table}
) e 
"""\
.format(tmp_nested_event_table=tmp_nested_event_table)
  
flat_event_df = spark.sql(query) 

tmp_flat_event_table = "tmp_flat_" + kafka_event_name
flat_event_df.createOrReplaceTempView(tmp_flat_event_table)

# COMMAND ----------

stream_write_query_name = kafka_event_name + "_stream_write_query_name"
s3_checkpoint_event_dir = s3_prefix + s3_bucket + "/" + s3_checkpoint_base_dir + "/" + kafka_event_name
s3_data_event_dir = s3_prefix + s3_bucket + "/" + s3_table_base_dir + "/" + kafka_event_name

ws_df = flat_event_df.writeStream\
.queryName(stream_write_query_name)\
.outputMode("append")\
.format("delta")\
.option("path", s3_data_event_dir)\
.option("checkpointLocation", s3_checkpoint_event_dir)\
.partitionBy("yyyy", "mm", "dd")\
.start(s3_data_event_dir)


# COMMAND ----------

# display(df_product_impression)
