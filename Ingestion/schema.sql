CREATE SCHEMA IF NOT EXISTS msd;

DROP TABLE IF EXISTS msd.raw_csv;

CREATE EXTERNAL TABLE IF NOT EXISTS msd.raw_csv
(
  YearStart INT,
  YearEnd INT,
  LocationAbbr STRING,
  LocationDesc STRING,
  Datasource STRING,
  Class STRING,
  Topic STRING,
  Question STRING,
  Data_Value_Unit STRING,
  Data_Value_Type STRING,
  Data_Value FLOAT,
  Data_Value_Alt FLOAT,
  Data_Value_Footnote_Symbol STRING,
  Data_Value_Footnote STRING,
  Low_Confidence_Limit FLOAT,
  High_Confidence_Limit  FLOAT,
  Sample_Size INT,
  Total STRING,
  Age STRING,
  Gender STRING,
  Race STRING,
  GeoLocation STRING,
  ClassID STRING,
  TopicID STRING,
  QuestionID STRING,
  DataValueTypeID STRING,
  LocationID STRING,
  StratificationCategory1 STRING,
  Stratification1 STRING,
  StratificationCategoryId1 STRING,
  StratificationID STRING
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/msd.db/raw_csv'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1');

DROP TABLE IF EXISTS msd.yearly_avg_all_age;

CREATE EXTERNAL TABLE IF NOT EXISTS msd.yearly_avg_all_age
(
  question STRING,
  year STRING, 
  age STRING, 
  avg_value STRING
)
STORED AS PARQUET
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/msd.db/yearly_avg_all_age';


DROP TABLE IF EXISTS msd.yearly_avg_female;

CREATE EXTERNAL TABLE IF NOT EXISTS msd.yearly_avg_female
(
  question STRING,
  year STRING, 
  avg_value STRING
)
STORED AS PARQUET
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/msd.db/yearly_avg_female';
