package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputFolder;

		inputPath = "Input";
		outputFolder = "Output";

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
		JavaRDD<String> inputRDD = sc.textFile(inputPath); //.cache()

		// ItemID,Name,Category,FirstTimeInCatalog
		// ID1,t-shirt-winter,Clothing,2001/03/01-12:00:00
		
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
