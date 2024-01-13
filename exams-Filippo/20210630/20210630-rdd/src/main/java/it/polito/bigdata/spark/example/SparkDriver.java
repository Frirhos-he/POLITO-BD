package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		String outputFolder1;
		String outputFolder2;

		inputPath = args[0];
		inputPath2 = args[1];
		outputFolder1 = args[2];
		outputFolder2 = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 -
		// Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkConf conf = new SparkConf()
				.setAppName("Spark Exam")
				.setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/////////////////////////////// CODE HERE/////////////////////////////////
		JavaRDD<String> inputRDD = sc.textFile(inputPath).cache();

		JavaRDD<String> filteredRDD = inputRDD.filter(e -> {
			String[] fields = e.split(",");
			String year = fields[3].split("/")[0];

			return year.equals("2020") && fields[6].equals("T");
		});

		// modelId, <max,min>
		filteredRDD
				.mapToPair(
						e -> {
							String[] fields = e.split(",");
							Integer price = Integer.parseInt(fields[5]);
							return new Tuple2<String, Tuple2<Integer, Integer>>(fields[2],
									new Tuple2<Integer, Integer>(price, price));
						})
				.reduceByKey(
						(e1, e2) -> {
							int max = Integer.MIN_VALUE;
							if (e1._1() > e2._1()) {
								max = e1._1();
							} else {
								max = e2._1();
							}

							int min = Integer.MAX_VALUE;
							if (e1._2() < e2._2()) {
								min = e1._2();
							} else {
								min = e2._2();
							}

							return new Tuple2<Integer, Integer>(max, min);
						})
				.filter(e -> {
					return (e._2()._1() - e._2()._2() > 5000);
				})
				.keys()
				.saveAsTextFile(outputFolder1);
		;

		///////////////////////////// PART 2 /////////////////////////////
		JavaPairRDD<String, String> bikesRDD = sc.textFile(inputPath2)
				.mapToPair(e -> {
					return new Tuple2<String, String>(e.split(",")[0], e.split(",")[2]);
				});

		inputRDD.mapToPair(
				e -> {
					String[] fields = e.split(",");
					return new Tuple2<String, Integer>(fields[2], 1);
				})
				.fullOuterJoin(bikesRDD)
				.mapToPair(
						e -> {
							String k = e._1() + "," + e._2()._2();
							return new Tuple2<String, Integer>(k, 1);
						})
				.reduceByKey(
						(e1, e2) -> {
							return e1 + e2;
						})
				.filter(
						e -> {
							return e._2() <= 10;
						})
				.mapToPair(
					e -> {
						return new Tuple2<String, Integer> (e._1().split(",")[1], 1);
					}
				)
				.reduceByKey(
					(e1, e2) -> {return e1+e2;}
				)
				.filter(e-> e._2() >= 3)
				.saveAsTextFile(outputFolder2);

		// Close the Spark session
		sc.close();
	}
}
