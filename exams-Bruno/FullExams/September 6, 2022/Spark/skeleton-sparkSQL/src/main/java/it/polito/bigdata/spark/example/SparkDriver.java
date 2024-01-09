package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import static org.apache.spark.sql.functions.max;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPathCompanies;
		String inputPathDataCenters;
		String inputPathDailyPowerConsumptions;

		Double threshold;
		String outputFolder;

		inputPathCompanies = args[0];
		inputPathDataCenters = args[1];
		inputPathDailyPowerConsumptions = args[2];

		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkSession ss = SparkSession.builder()
								.master("local")
								.appName("Exam - Template")
								.getOrCreate();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkContext sc = ss.sparkContext();

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + sc.applicationId());
		System.out.println("******************************");

		// Company.txt has the following format CodC,CompanyName,Headquarters-Country
		Dataset<Row> companiesDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathCompanies);
	
		//  DataCenters.txt has the following format CodDC,CodC,City,Country,Continent
		Dataset<Row> dataCentersDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathDataCenters)
				.withColumnRenamed("_c0", "codDC")
				.withColumnRenamed("_c4", "Continent")
				.select("codDC","Continent");

		/*
		Data centers with an increasing number of dates with high power consumption 
		(kWh>1000) in the last year. The application considers only the lines of 
		DailyPowerConsumption.txt associated with high power consumption (kWh greater 
		than 1000) and selects the data centers for which the number of dates with high power 
		consumption in the year 2021 is greater than the number of dates with high power 
		consumption in the year 2020. Store the identifiers (CodDC) of the selected data 
		centers in the output HDFS folder.*/

		// DailyPowerConsumption.txt has the following format CodDC,Date,kWh
		Dataset<Row> dailyPowerConsumptionDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathDailyPowerConsumptions)
				.withColumnRenamed("_c0", "codDC")
				.withColumnRenamed("_c1", "Date")
				.withColumnRenamed("_c2", "kwh");
		dailyPowerConsumptionDF.show();
		Dataset<Row> daily1000kWhDF = dailyPowerConsumptionDF.filter("kwh>1000");
		daily1000kWhDF.show();
		Dataset<Row> powerConsumption2021 = daily1000kWhDF.filter("Date between '2020/01/01' and '2021/12/31'");

		ss.udf().register("isyear2021", (String value) -> {
			if (value.contains("2021"))
				return 1;
			else
				return 0;
		}, DataTypes.IntegerType);

		Dataset<Row> powerConsumptionis21 = powerConsumption2021.selectExpr("codDC","isyear2021(Date) as year2021");
		powerConsumptionis21.show();
		Dataset<Row> resultDF = powerConsumptionis21.groupBy("codDC").avg("year2021").withColumnRenamed("avg(year2021)", "avg");
		Dataset<Row> resultFilteredRD = resultDF.filter("avg>0.5").select("codDC");
		//resultFilteredRD.coalesce(1).write().option("header",false).format("csv").save(outputFolder);
		resultDF.show();
		


		/*
		 * 1. Dates with high power consumption in many data centers. The first part of this
		application selects the dates on which at least 90% of the data centers consumed at 
		least 1000 kWh (at least 1000 kWh in each data center). Store the selected dates in 
		the first HDFS output folder (one date per line).
		 */
			
		 ss.udf().register("consumed1000kwh", (Integer value) -> {
			 if(value>1000){
				return 1;
			 } else return 0;
		 }, DataTypes.IntegerType);

		 Dataset<Row> powerConsumptionDF = dailyPowerConsumptionDF.selectExpr("date","consumed1000kwh(kwh) as consumed");
		 powerConsumptionDF.show();
		 Dataset<Row> groupedPowerConsumptionDF = powerConsumptionDF.groupBy("date").avg("consumed").withColumnRenamed("avg(consumed)", "perc");
		 groupedPowerConsumptionDF.show();
		 Dataset<Row> datesDF = groupedPowerConsumptionDF.filter("perc>0.9").select("date");
		// datesDF.coalesce(1).write().format("csv").option("header",false).save(outputFolder);

		 /*2. Continent(s) with the highest average power consumption per data center in the year 
			2021 and the highest number of data centers. This second part of the application 
			selects the continent(s) with (i) the highest average power consumption per data center 
			in the year 2021 and (ii) the highest number of data centers. This second part of the 
			application selects only the continent(s) satisfying both constraints. Given a continent, 
			its average power consumption per data center in 2021 is the sum of the power 
			consumption of all its data centers in 2021 divided by the number of data centers in 
			that continent. In case of a tie, the application selects all the continents that satisfy both 
			constraints. Store the selected continent(s) in the second HDFS output folder (one 
			continent per output line).
 		*/

		Dataset<Row> powerConsumptionDF2021 = dailyPowerConsumptionDF.filter("Date between '2021/01/01' and '2021/12/31'");
		//look for the highest average power consumption per data center in the year 2021
		Dataset<Row> powerConsumptionCodDF2021 = powerConsumptionDF2021.groupBy("codDC").avg("kwh");
		Row maxPowerConsumption = powerConsumptionCodDF2021.agg(max("avg(kwh)").alias("max")).first();
		//maxPowerConsumption.show();
		double maxPower = maxPowerConsumption.getDouble(maxPowerConsumption.fieldIndex("max"));
		Dataset<Row> powerCentersDF2021 = powerConsumptionCodDF2021.filter("avg(kwh)="+maxPower);

		//Select the continents
		Dataset<Row> continentDF = powerCentersDF2021.join(dataCentersDF, dataCentersDF.col("codDC").equalTo(powerCentersDF2021.col("codDC")))
									.select("Continent");



		 /*

	Dataset<Row> watchedDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathWatched)
				.withColumnRenamed("_c0", "WatchedUserId")
				.withColumnRenamed("_c1", "movieId")
				.select("WatchedUserId","movieId");
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		watchedDF.printSchema();
		watchedDF.show();

		
		// Read the content of the movies file
		// Select only movieid and genre
		Dataset<Row> moviesDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathMovies)
				.withColumnRenamed("_c0", "movieId")
				.withColumnRenamed("_c2", "WatchedMovieGenre")
				.select("movieId","WatchedMovieGenre");
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		moviesDF.printSchema();
		moviesDF.show();
		
		// Join watched movie with movies
		// Select only userid and movie genre
		Dataset<Row> usersWatchedGenresDF =	watchedDF
				.join(moviesDF, watchedDF.col("movieId").equalTo(moviesDF.col("movieId")))
				.select("WatchedUserId", "WatchedMovieGenre");
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		usersWatchedGenresDF.printSchema();
		usersWatchedGenresDF.show();
		
		
		// Read the content of the preferences
		// Add a new constant column that is needed in the left outer join    
		Dataset<Row> userLikedGenresDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathPreferences)
				.withColumnRenamed("_c0", "LikedUserId")
				.withColumnRenamed("_c1", "LikedMovieGenre")
				.selectExpr("LikedUserId", "LikedMovieGenre");
		
		// printSchema and show only for debug purposes
		// They can be removed
		userLikedGenresDF.printSchema();
		userLikedGenresDF.show();

		
		Dataset<Row> userWatchedLikedGenresDF = userLikedGenresDF.join(usersWatchedGenresDF,usersWatchedGenresDF.col("WatchedUserId").equalTo(userLikedGenresDF.col("LikedUserId"))
		.and(usersWatchedGenresDF.col("WatchedMovieGenre").equalTo(userLikedGenresDF.col("LikedMovieGenre")))
		,"rightouter");
		userWatchedLikedGenresDF.show();

		ss.udf().register("notLikedUDF", (String value) -> {
			if (value==null)
				return 1;
			else
				return 0;
		}, DataTypes.IntegerType);
		
		Dataset<Row> selectLikedOrNotLikedDataset = userWatchedLikedGenresDF
													.selectExpr("WatchedUserId","notLikedUDF(LikedMovieGenre) as notLiked");
											selectLikedOrNotLikedDataset.show();
		Dataset<Row> avgLikedDataset = selectLikedOrNotLikedDataset.groupBy("WatchedUserId").agg(avg("notLiked"));
		avgLikedDataset.show();
		Dataset<Row> filteredDataset = avgLikedDataset.filter("avg(notLiked) > " + threshold);
		filteredDataset.show();
		filteredDataset.coalesce(1).write().format("csv").option("header",false).save(outputPath);


		Dataset<Row> joinedWatchedUserIdGenreDF = watchedDF.join(movieGenreDF, movieGenreDF.col("movieId").equalTo(watchedDF.col("movieId"))).select("userId","movieGenre");
		// userid, genre
		joinedWatchedUserIdGenreDF.show();

		JavaPairRDD<String,String> usersWatchedGenresPairRDD = joinedWatchedUserIdGenreDF.toJavaRDD().mapToPair(row -> 
		new Tuple2<String,String>((String)row.getAs("userId"), (String)row.getAs("movieGenre")));

		JavaPairRDD<String,String> usersPreferredGenresPairRDD = userGenreDF.toJavaRDD().mapToPair(row -> 
		new Tuple2<String,String>((String)row.getAs("userId"), (String)row.getAs("Prefferedgenre")));

		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> resultRDD = usersWatchedGenresPairRDD.cogroup(usersPreferredGenresPairRDD);
		
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> resultFilterRDD = resultRDD.filter(pair->{

			Iterable<String> watchedGenres = pair._2()._1();
			Iterable<String> preferredGenres = pair._2()._2();

			ArrayList<String> preferredGenresList =  new ArrayList<String>();
			for(String genre : preferredGenres){
				preferredGenresList.add(genre);
			}
			int counterDoesnLike = 0;
			int counterTotalPresent = 0;
			for(String genre : watchedGenres){
				if(!preferredGenresList.contains(genre)){
					counterDoesnLike++;
				}
				counterTotalPresent++;
			}
			if((double) ((double)counterDoesnLike/(double)counterTotalPresent) > threshold){
				return true;
			}else{
				return false;
			}

		});

		JavaRDD<String> selectedRDD = resultFilterRDD.map(pair ->{
			return pair._1();
		});

		selectedRDD.saveAsTextFile(outputPath);

		*/

		// Close the Spark session
		ss.stop();
	}
}
