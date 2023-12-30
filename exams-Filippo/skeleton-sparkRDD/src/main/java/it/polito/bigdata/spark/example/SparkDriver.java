package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputFolder;

		inputPath = args[0];
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkConf conf=new SparkConf()
					.setAppName("Spark Exam")
					.setMaster("local")
					;
								
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		///////////////////////////////CODE HERE/////////////////////////////////
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		
		// Close the Spark session
		sc.close();
	}
}
