# Remove folders of the previous run
hdfs dfs -rm -r ex23Bis_data
hdfs dfs -rm -r ex23Bis_out
hdfs dfs -rm -r ex23Bis_temp

# Put input data collection into hdfs
hdfs dfs -put ex23Bis_data


# Run application
hadoop jar target/Exercise23Bis-1.0.0.jar it.polito.bigdata.hadoop.exercise23bis.DriverBigData ex23Bis_data/  ex23Bis_out/ User2 



