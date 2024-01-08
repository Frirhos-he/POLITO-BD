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
		outputFolder1 = args[1];
		outputFolder2 = args[2];
		inputPath2 = args[3];

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
		// airports.txt
		JavaRDD<String> airportsRDD = sc.textFile(inputPath2);

		// flights.txt
		JavaRDD<String> flightsRDD = sc.textFile(inputPath).cache();

		// join
		JavaPairRDD<String, String> airportsCountryRDD = airportsRDD.mapToPair(
			e -> {
				String[] fields = e.split(",");
				String airportId = fields[0];
				String name = fields[1];
				String country = fields[3];
				return new Tuple2<String, String>(airportId, name+","+country);
			}
		).filter(
			e -> e._2().split(",")[1].toLowerCase().equals("germany")
		)
		;

		JavaPairRDD<String, String> airportFlightsRDD = flightsRDD.mapToPair(
			e -> {
				String[] fields = e.split(",");
				String company = fields[1];
				String destId = fields[6];
				String delay = fields[7];
				String occup = fields[9];
				String booked = fields[10];
				return new Tuple2<String, String>(destId, company+","+delay+","+occup+","+booked);
			}
		)
		;

		//--------------------------------------------------------------------------
		JavaPairRDD<String, Tuple2<String,String>> joinedRDD = airportFlightsRDD.join(airportsCountryRDD);

		joinedRDD
		.mapToPair(
			e -> {
				String delay = e._2()._1().split(",")[1];
				String key = e._2()._1().split(",")[0] + "," + e._2()._2().split(",")[0];
				SumCount value = new SumCount(0,1);
				if(delay.compareTo("15")>= 0) {
					value.sum += 1;
				}
				return new Tuple2<String, SumCount> (key, value);
			}
		)
		.reduceByKey(
			(a,b) -> {
				return new SumCount(a.sum+b.sum, a.count+b.count);
			}
		)
		.mapToPair(
			e -> {
				return new Tuple2<Integer, String>(e._2().count, e._1());
			}
		)
		.sortByKey()
		.saveAsTextFile(outputFolder1)
		;


		//--------------------------------------------------------------------------

		JavaPairRDD<String, SumSumCount> flightsInfoRDD = flightsRDD.mapToPair(
			e -> {
				String[] fields = e.split(",");
				String dep = fields[5];
				String arr = fields[6];
				String cancel = fields[8];
				String nSeats = fields[9];
				String nOccupiedSeats = fields[10];
				Double res = Double.parseDouble(nOccupiedSeats) / Double.parseDouble(nSeats);
				SumSumCount ssc = new SumSumCount(1, res, cancel.toLowerCase().equals("yes") ? 1 : 0);
				return new Tuple2<String, SumSumCount>(dep+","+arr, ssc);
			}
		);
		
		flightsInfoRDD.reduceByKey(
			(a,b) -> {
				return new SumSumCount(a.count+b.count, a.sumOverload+b.sumOverload, a.nCancelled+b.nCancelled);
			}
		)
		.filter(
			e -> {
				return e._2().sumOverload / e._2().count >= 0.99 && (double) e._2().nCancelled / e._2().count >= 0.05;
			}
		)
		.map(
			e -> {return e._1();}
		)
		.saveAsTextFile(outputFolder2);
		;

		// Close the Spark session
		sc.close();
	}
}
