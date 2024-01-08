package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {
    public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPathServers;
		String inputPathPatchesServers;
		String outputPathPart1;
		String outputPathPart2;

		inputPathServers = "exam_ex2_data/Servers.txt";
		inputPathPatchesServers = "exam_ex2_data/PatchedServers.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> patchesRDD = sc.textFile(inputPathPatchesServers).cache();

		JavaRDD<String> RDD1819 = patchesRDD.filter(line->{
			String[] fields = line.split(",");
			String date = fields[2];

			if(date.startsWith("2018") || date.startsWith("2019")) return true;
			else return false;

		});
		JavaRDD<String,Count1819> sid1819 = RDD1819.mapToPair(line->{
			String []fields = line.split(",");
			String date = fields[2];
			Count1819 counts;
			if(date.startsWith("2018")){
				 counts = new Count1819(1, 0);
			}else{
				counts = new Count1819(0, 1);
			}
			return new Tuple2<String,Count1819>() 
		})
		sc.close();
		
    }
}
