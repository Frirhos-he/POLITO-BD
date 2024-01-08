package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkSession ss = SparkSession.builder()
								.master("local")
								.appName("Exam - Template")
								.getOrCreate();

		SparkContext sc = ss.sparkContext();

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + sc.applicationId());
		System.out.println("******************************");


		/*
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
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
		Dataset<Row> powerConsumption2021 = daily1000kWhDF.filter("Date between '2020/01/01' and '2021/12/31'");
		Row maxPowerConsumptionDF2021 = powerConsumptionDF2021.groupBy("codDC").avg("kwh").select("avg(kwh) as avg").agg(max("avg")).alias("max").first();
		int maxPower = maxPowerConsumptionDF2021.getInt(maxPowerConsumptionDF2021.fieldIndex("max"));
		*/

		// Close the Spark session
		ss.stop();
	}
}
