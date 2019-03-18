# MSD_BigData_Interview_Project

## Requirements
This script has been tested on Ubuntu 18.04.

Please install docker before you run this script. 

## Instructions
1. Run Hortonworks Data Platform Docker build script
   ```bash
   $ cd HDP_3.0.1_docker-deploy-scripts
   $ bash docker-deploy-hdp30.sh
   ```
2. Start the HDP docker.
   ```bash
   $ docker start sandbox-hdp
   $ docker start sandbox-proxy
   ```
3. Restart all of the Hadoop service by accessing the ambari dashboard
   ```bash
   http://localhost:8080
   Username: raj_ops
   Password: raj_ops
   ```
4. Copy these files to the HDP docker /root folder.
   ```bash
   Ingestion/copy_processed_files.sh
   Ingestion/ingestion.sh
   Ingestion/schema.sql
   ETL_Spark/build/AllAgeJob.jar
   ETL_Spark/build/FemaleJob.jar
   ```
   Command
   ```bash
   $ scp -P 2222 Ingestion/* root@localhost:~/
   $ scp -P 2222 ETL_Spark/build/* root@localhost:~/
   ```
5. Run the following commands sequentially.
   ```bash
   # Enter sandbox-hdp docker
   $ ssh -p 2222 root@localhost
   $ bash ingestion.sh
   $ hive -f schema.sql
   $ spark-submit --class com.msd.AllAgeApp --master yarn --deploy-mode client AllAgeJob.jar
   $ spark-submit --class com.msd.FemaleApp --master yarn --deploy-mode client FemaleJob.jar
   $ bash copy_processed_files.sh
   ```
6. You can verify the result by query the hive tables (from inside the sandbox docker)
   ```bash
   $ hive
   ```

   First Output
   ```sql
   SELECT * FROM msd.yearly_avg_all_age
   ```
   Second Output
   ```sql
   SELECT * FROM msd.yearly_avg_female
   ```
