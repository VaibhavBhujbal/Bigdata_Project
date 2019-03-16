#!/bin/bash

sudo -u root hdfs dfs -rm -R /apps/spark/warehouse/msd.db
sudo -u root hdfs dfs -mkdir /apps/spark/warehouse/msd.db
sudo -u root hdfs dfs -mkdir /apps/spark/warehouse/msd.db/raw_csv

wget -O rows.csv "https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD"
hdfs dfs -put rows.csv /warehouse/tablespace/managed/hive/msd.db/raw_csv/rows.csv
rm rows.csv

sudo -u root hdfs dfs -chown -R hive /warehouse/tablespace/managed/hive/msd.db

hive -f schema.sql

echo "Done"
