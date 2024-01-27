package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath1;
		String inputPath2;
		String outputFolder1;
		String outputFolder2;

		inputPath1 = args[0];
		inputPath2 = args[1];
		outputFolder1 = args[2];
		outputFolder2 = args[3];

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
		JavaRDD<String> patchedRDD = sc.textFile(inputPath1).cache();
		JavaRDD<String> modelRDD = sc.textFile(inputPath2);

		JavaPairRDD<String, String> modelPairRDD = modelRDD.mapToPair(e -> {
			String[] fields = e.split(",");
			String id = fields[0];
			String model = fields[1];
			return new Tuple2<String, String>(id, model);
		})
		.cache();

		patchedRDD
		.filter(e -> e.split(",")[2].startsWith("2018") || e.split(",")[2].startsWith("2019"))
		.mapToPair(e -> {
			String[] fields = e.split(",");
			String id = fields[0];
			int year = Integer.parseInt(fields[2].split("/")[0]);
			return new Tuple2<String, Integer>(id+"_"+year, 1);
		})
		.reduceByKey(
			(a,b) -> {
				// the result is the total number of patches applied for SID_year
				return a+b;
			}
		)
		.mapToPair(
			e -> {
				String[] fields = e._1().split("_");
				String id = fields[0];
				String year = fields[1];
				return new Tuple2<String, Tuple2<Integer, Integer>>(id, new Tuple2<Integer, Integer>(Integer.parseInt(year), e._2()));
			}
		)
		.reduceByKey(
			(a,b) -> {
				// the result is the fraction wrt both years
				double result = .0;
				if(a._1() == 2018) {
					result = b._2()/a._2();
				} else {
					result = a._2()/b._2();
				}
				int ret = result<0.5 ? 1 : 0;
				return new Tuple2<Integer, Integer>(a._1(), ret);
			}
		)
		.filter(e -> e._2()._2() == 1)
		.join(modelPairRDD)
		// .mapToPair(e -> {
		// 	return new Tuple2<String, String>(e._1(),e._2()._2());
		// })
		.mapValues(e -> e._2())
		.saveAsTextFile(outputFolder1);
		;

		/////////////////////////////// PART 2 /////////////////////////////////
		JavaPairRDD<String, Integer> notCompliantServersRDD =patchedRDD.mapToPair(e -> {
			String[] fields = e.split(",");
			String date = fields[2];
			String id = fields[0];
			return new Tuple2<String, Integer>(id+","+date, 1);
		})
		.reduceByKey(
			(a,b) -> {
				return a+b;
			}
		)
		.filter(e -> e._2()>1)
		.mapToPair(e -> {
			return new Tuple2<String, Integer>(e._1().split(",")[0], 1);
		})
		;
		
		JavaPairRDD<String, String> resRDD = modelPairRDD.subtractByKey(notCompliantServersRDD);
		resRDD.saveAsTextFile(outputFolder2);

		System.out.println("Distinct models = " + resRDD.values().distinct().count());
		
		
		// Close the Spark session
		sc.close();
	}
}
