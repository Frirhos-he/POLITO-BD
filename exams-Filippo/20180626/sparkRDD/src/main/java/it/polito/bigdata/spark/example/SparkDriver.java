package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		Double CPUthr;
		Double RAMthr;
		String outputA;
		String outputB;

		inputPath = args[0];
		CPUthr = Double.parseDouble(args[1]);
		RAMthr = Double.parseDouble(args[2]);
		outputA = args[3];
		outputB = args[4];

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkConf conf=new SparkConf()
					.setAppName("Spark Exam")
					.setMaster("local")
					;
								
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		///////////////////////////////CODE HERE/////////////////////////////////
		// Timestamp,VSID,CPUUsage%,RAMUsage%
		// 2018/03/01,15:40,VS1,10.5,0.5
		JavaRDD<String> inputRDD = sc.textFile(inputPath)
		.filter(
			line -> {
				String[] dateFields = line.split(",")[0].split("/");
				return dateFields[0].equals("2018") && dateFields[1].equals("05");
			}
		)
		.cache();
		
		// PART 1
		inputRDD
		.mapToPair(
			line -> {
				String[] fields = line.split(",");
				SumCount sumcount = new SumCount(Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), 1);
				String pair = fields[2] + "_" + fields[1].split(":")[0];
				return new Tuple2<String, SumCount>(pair, sumcount);
			}
		)
		.reduceByKey(
			(e1, e2) -> {
				return new SumCount(e1.cpuSum+e2.cpuSum, e1.ramSum+e2.ramSum, e1.count+e2.count);
			}
		)
		.filter(
			e -> {
				Double cpuAvg = e._2().cpuSum / e._2().count;
				Double ramAvg = e._2().ramSum / e._2().count;
				
				return cpuAvg >= CPUthr && ramAvg >= RAMthr;
			}
		)
		.keys()
		.saveAsTextFile(outputA);

		// PART 2
		JavaPairRDD<String, Double> maxMin =inputRDD.mapToPair(
			line -> {
				String[] fields = line.split(",");
				String pair = fields[2] + "_" + fields[0] +"," + fields[1].split(":")[0];
				Double cpuUsage = Double.parseDouble(fields[3]);
				                            //    max  ,  min
				return new Tuple2<String, Double> (pair, cpuUsage);
			}
		)
		.reduceByKey(
			(e1, e2) -> {
				double max = Double.MIN_VALUE;
				if(e1 > e2) {
					max = e1;
				} else {
					max = e2;
				}
				return max;
			}
		)
		.mapToPair(
			e -> {
				return new Tuple2<String, Double> (e._1().split(",")[0], e._2());
			}
		)
		;

		maxMin
		.mapToPair(
			e -> {
				SumCritical sumcritical = new SumCritical(0,0);
				if(e._2() >= 90.0) {
					sumcritical.cpuCriticalHoursMax = 1;
				} else if(e._2() < 10.0) {
					sumcritical.cpuCriticalHoursMin = 1;
				}
				return new Tuple2<String, SumCritical>(e._1(), sumcritical);
			}
		)
		.reduceByKey(
			(e1, e2) -> {
				return new SumCritical(e1.cpuCriticalHoursMax + e2.cpuCriticalHoursMax, e1.cpuCriticalHoursMin + e2.cpuCriticalHoursMin);
			}
		)
		.filter(
			line -> {
				return line._2().cpuCriticalHoursMax >= 8 && line._2().cpuCriticalHoursMin >= 8;
			}
		)
		.keys()
		.saveAsTextFile(outputB);

		
		// Close the Spark session
		sc.close();
	}
}
