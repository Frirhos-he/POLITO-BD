package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath1;
		String inputPath2;

		String outputFolder;

		inputPath = args[0];
		inputPath1 = args[1];
		inputPath2 = args[2];
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkConf conf=new SparkConf()
					.setAppName("Spark Exam")
					.setMaster("local")
					;
								

		SparkSession ss = SparkSession.builder()
								.master("local")
								.appName("Exam - Template")
								.getOrCreate();
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		Dates with high power consumption in many data centers. The first part of this
		application selects the dates on which at least 90% of the data centers consumed at 
		least 1000 kWh (at least 1000 kWh in each data center). Store the selected dates in 
		the first HDFS output folder (one date per line). 
		*/

		// CodDC,Date,kWh
		JavaRDD<String> datacenterRDD = sc.textFile(inputPath); //.cache()
		
		JavaRDD<String> data1000kwRDD  = datacenterRDD.filter(line->{
			String[] fields = line.split(",");
			Double kwh = Double.parseDouble(fields[2]);
			if(kwh>1000)return true;
			else return false;
		});
		JavaPairRDD<String,Iterable<String>> pairIterableFilteredRDD = data1000kwRDD.mapToPair(line->{ //Date, codDC
			String[] fields = line.split(",");

			return new Tuple2<String,String>(fields[1],fields[0]);
		}).groupByKey();

		JavaPairRDD<String,Iterable<String>> pairIterableRDD = datacenterRDD.mapToPair(line->{ //Date, codDC
			String[] fields = line.split(",");

			return new Tuple2<String,String>(fields[1],fields[0]);
		}).groupByKey();
	
		JavaRDD dataHighPowerRDD = pairIterableFilteredRDD.join(pairIterableRDD).map(pair->{
			double size= 0;
			double total= 0;
			for(String value: pair._2()._1())size++;
			for(String value: pair._2()._2())total++;
			if(size>0.9*total)return pair._1();
			else return null;
		});
		dataHighPowerRDD.saveAsTextFile(outputFolder);



		Dataset<Row> dfDataset = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath1);
		Dataset<Row> dfDailyPowerConsumption = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.option("timestampFormat","yyyy/MM/dd")
				.load(inputPath);
			
		// DailyPowerConsumption.txt has the following format CodDC,Date,kWh
		// DataCenters.txt has the following format CodDC,CodC,City,Country,Continent
		// Company.txt has the following format CodC,CompanyName,Headquarters-Country
		
		/*Continent(s) with the highest average power consumption per data center in the year 
		2021 and the highest number of data centers. This second part of the application 
		selects the continent(s) with (i) the highest average power consumption per data center 
		in the year 2021 and (ii) the highest number of data centers. This second part of the 
		application selects only the continent(s) satisfying both constraints. Given a continent, 
		its average power consumption per data center in 2021 is the sum of the power 
		consumption of all its data centers in 2021 divided by the number of data centers in 
		that continent. */

		Dataset<Row> dfDatasetFiltered = dfDataset.filter("Year>2021");

		Dataset<Row> nonBannedProfiles= dsProfiles.join(dsBanned,
										dsProfiles.col("uid").equalTo(dsBanned.col("uid")),
										"leftanti");
		/*
		JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));
		JavaRDD<String> ipRDD = googleRDD.map(logLine -> {
			String[] parts = logLine.split(" ");
			String ip = parts[0];

			return ip;
		});
		JavaRDD<String> ipDistinctRDD = ipRDD.distinct();
				JavaPairRDD<String, String> questionsPairRDD = questionsRDD.mapToPair(question -> {

			String questionID;
			String questionText;
			Tuple2<String, String> pair;

			// Split the line in fields
			String[] fields = question.split(",");

			// fields[0] contains the questionId
			questionID = fields[0];

			// fields[2] contains the text of the question
			questionText = fields[2];

			pair = new Tuple2<String, String>(questionID, questionText);

			return pair;
		});
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> questionsAnswersPairRDD = 
		questionsPairRDD.cogroup(answersPairRDD);
		percentageStationsPairRDD.sortByKey(false).saveAsTextFile(outputPath);
		// Create a Broadcast version of the neighbors list
		final Broadcast<HashMap<String, List<String>>> neighborsBroadcast = sc.broadcast(neighbors);
		List<String> nCurrentStation = neighborsBroadcast.value().get(stationId);
		List<Tuple2<Integer,String>> topKCriticalSensors=sortedNumCriticalValuesSensorRDD.take(k);
		List<Double> topPM10Value = pm10ValuesRDD.top(3); //pm10Values is JavaRDD
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World", "Spark is awesome"));
		JavaRDD<String> flatMappedRDD = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		*/
		// Close the Spark session
		sc.close();
	}
}
