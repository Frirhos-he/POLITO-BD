# Remove folders of the previous run
hdfs dfs -rm -r ex23_data
hdfs dfs -rm -r tempFolderOutJob1
hdfs dfs -rm -r ex23_out

# Put input data collection into hdfs
hdfs dfs -put ex23_data


# Run application
hadoop jar target/Ex23-1.0.0.jar it.polito.bigdata.hadoop.exercise23v2.DriverBigData ex23_data/  ex23_out/ User2


