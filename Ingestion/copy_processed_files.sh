#!/bin/bash

sudo -u hive hdfs dfs -rm -R /warehouse/tablespace/managed/hive/msd.db/yearly_avg_all_age
sudo -u hive hdfs dfs -cp /apps/spark/warehouse/msd.db/yearly_avg_all_age /warehouse/tablespace/managed/hive/msd.db/yearly_avg_all_age

sudo -u hive hdfs dfs -rm -R /warehouse/tablespace/managed/hive/msd.db/yearly_avg_female
sudo -u hive hdfs dfs -cp /apps/spark/warehouse/msd.db/yearly_avg_female /warehouse/tablespace/managed/hive/msd.db/yearly_avg_female

echo "Done"
