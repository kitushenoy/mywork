CREATE EXTERNAL TABLE ${hiveconf:x} ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED as INPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION ${hiveconf:y}
TBLPROPERTIES ('avro.schema.url'=${hiveconf:z});
