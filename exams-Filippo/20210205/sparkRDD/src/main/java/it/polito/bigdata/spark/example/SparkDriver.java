package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {
	public static void main(String[] args) {
		String inProdPlants;
		String inRobots;
		String inFailures;
		String outputPathPart1;
		String outputPathPart2;
		inProdPlants = "ProductionPlants.txt";
		inRobots = "Robots.txt";
		inFailures = "Failures.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2");
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		/* Write your code here */

		JavaRDD<String> robotsRDD = sc.textFile(inRobots);
		JavaRDD<String> failuresRDD = sc.textFile(inFailures);

		JavaPairRDD<String, Integer> rf50 = failuresRDD
				.filter(e -> e.split(",")[2].startsWith("2020"))
				.mapToPair(
						e -> {
							String rid = e.split(",")[0];
							return new Tuple2<String, Integer>(rid, 1);
						})
				.reduceByKey(
						(a, b) -> {
							return a + b;
						})
				.filter(e -> e._2() >= 50);

		JavaPairRDD<String, String> robotPlantRDD = robotsRDD.mapToPair(
				e -> {
					String rid = e.split(",")[0];
					String pid = e.split(",")[1];

					return new Tuple2<String, String>(rid, pid);
				});

		rf50.join(robotPlantRDD)
				.map(e -> {
					return e._2()._2();
				})
				.saveAsTextFile(outputPathPart1);

		///////////////// PART 2 ///////////////////

		JavaPairRDD<String, Integer> plantWithFailures = failuresRDD.mapToPair(e -> {
			String[] fields = e.split(",");
			return new Tuple2<String, Integer>(fields[0], 1);
		}).reduceByKey(
				(a, b) -> {
					return 1;
				}).join(robotPlantRDD)
				.mapToPair(e -> {
					return new Tuple2<String, Integer>(e._2()._2(), 1);
				})
				.reduceByKey(
						(a, b) -> {
							return a + b;
						});

		JavaRDD<String> plantsRDD = sc.textFile(inProdPlants);
		plantsRDD.mapToPair(e -> {
			return new Tuple2<String, Integer>(e.split(",")[0], 0);
		}).subtractByKey(plantWithFailures)
				.union(plantWithFailures)
				.saveAsTextFile(outputPathPart2);
	}
}
