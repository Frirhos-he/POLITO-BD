package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import java.util.*;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath1;
		String outputPath2;

		inputPath = args[0];
		outputPath1 = args[1];
		outputPath2 = args[2];

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
		JavaRDD<String> inputRDD = sc.textFile(inputPath)
				.filter(t -> {
					String fields[] = t.split(",");
					return fields[1].split("/")[0].equals("2017");
				});

		JavaPairRDD<String, Tuple2<Double, Double>> pairRDD = inputRDD.mapToPair(e -> {
			String fields[] = e.split(",");
			String date = fields[1];
			String stock = fields[0];
			Double val = Double.parseDouble(fields[3]);
			return new Tuple2<String, Tuple2<Double, Double>>(stock + "," + date, new Tuple2<>(val, val));
		})
				.reduceByKey((e1, e2) -> {
					double max, min;
					if (e1._1() > e2._1()) {
						max = e1._1();
					} else {
						max = e2._1();
					}

					if (e1._2() < e2._2()) {
						min = e1._2();
					} else {
						min = e2._2();
					}

					return new Tuple2<Double, Double>(max, min);
				})
				.cache();

		pairRDD
				.filter(e -> {
					return (e._2()._1() - e._2()._2() > 10);
				})
				.mapToPair(e -> {
					String fields[] = e._1().split(",");
					return new Tuple2<String, Integer>(fields[0], 1);
				})
				.reduceByKey((e1, e2) -> {
					return e1 + e2;
				})
				.filter(e -> {
					return e._2() >= 1;
				})
				.saveAsTextFile(outputPath1)
		;

		///////////////////////// SECOND PART /////////////////////////

		pairRDD.flatMapToPair(
				e -> {
					List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
					list.add(new Tuple2<String, Double>(e._1(), e._2()._1() - e._2()._2()));
					list.add(new Tuple2<String, Double>(DateTool.previousDate(e._1()), e._2()._1() - e._2()._2()));
					return list.iterator();
				})
				.reduceByKey((e1, e2) -> {
					return Math.abs(e1-e2);
				})
				.filter(e -> e._2() <= 0.1)
				.keys()
				.saveAsTextFile(outputPath2);
		;
		// Close the Spark session
		sc.close();
	}
}
