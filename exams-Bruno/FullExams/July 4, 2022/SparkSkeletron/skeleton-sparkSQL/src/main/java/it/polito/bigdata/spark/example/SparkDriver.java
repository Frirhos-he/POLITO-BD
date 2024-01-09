package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.SparkContext;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathPatched;
		String inputPathApplied;
		Double threshold;
		String outputFolder;

		inputPathPatched = args[0];
		inputPathApplied = args[1];
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkSession ss = SparkSession.builder()
								.master("local")
								.appName("Exam")
								.getOrCreate();

		SparkContext sc = ss.sparkContext();

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + sc.applicationId());
		System.out.println("******************************");

		/*1. Patches of the operating system Ubuntu 2 that were applied on many servers on the 
		same date they were released. The first part of this application considers the patches 
		released for the operating system Ubuntu 2 and selects the patches that were applied 
		on at least 100 servers on the same date they were released (i.e., consider only the 
		cases with ApplicationDate equal to ReleaseDate). Store the identifiers (PIDs) of the 
		selected patches in the first HDFS output folder (one PID per line).
		*/

		// Patches.txt PID,ReleaseDate,OperatingSystem

		Dataset<Row> patchedDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathPatched)
				.withColumnRenamed("_c0", "PID")
				.withColumnRenamed("_c1", "releaseDate")
				.withColumnRenamed("_c2", "op");
		patchedDF.show();
		Dataset<Row> filteredPatchedDF = patchedDF.filter("op = 'Ubuntu2'");
				// AppliedPatches.txt PID,SID, ApplicationDate
		Dataset<Row> appliedPatchesDF = ss.read().format("csv")
				.option("header", false)
				.option("inferSchema", true)
				.load(inputPathApplied)
				.withColumnRenamed("_c0", "PID")
				.withColumnRenamed("_c1", "SID")
				.withColumnRenamed("_c2", "applicationDate");
		
			


		Dataset<Row> joinPatchedDF = filteredPatchedDF.join(appliedPatchesDF, appliedPatchesDF.col("PID").equalTo(filteredPatchedDF.col("PID"))
													.and(appliedPatchesDF.col("applicationDate").equalTo(filteredPatchedDF.col("releaseDate"))))
													.select(filteredPatchedDF.col("PID"),appliedPatchesDF.col("SID"),appliedPatchesDF.col("applicationDate"));
		
		Dataset<Row> groupedDatePIDDF = joinPatchedDF.groupBy("PID","applicationDate")
						.count()
						.filter("count >= 100")
						.select("PID");
		groupedDatePIDDF.show();
		groupedDatePIDDF.coalesce(1).write().format("csv").option("header",false).save(outputFolder);


		/*Number of months of the year 2021 without applied patches for each server. The 
		second part of this application computes for each server the number of months of the 
		year 2021 in which no patches have been applied. Store in the second HDFS output 
		folder the SID and the number of months of the year 2021 in which no patches have 
		been applied for each server (one pair (SID, number of months of the year 2021 in 
		which no patches have been applied to SID) per output line). The servers with 0 
		months of the year 2021 without applied patches are also stored in the second output 
		folder. */

		Dataset<Row> filterAppliedDF = appliedPatchesDF.filter("applicationDate between '2021/01/01' and '2021/12/31'");

		ss.udf().register("getMonth", (String date) -> {
			String[] fields = date.split("/");
			return fields[1];
		},DataTypes.StringType);
		
		Dataset<Row> addedSelectDF = filterAppliedDF.selectExpr("SID","getMonth(applicationDate) as month");
		
		ss.udf().register("getNoMonthCount", (Integer count) -> {
			return (12-count);
		},DataTypes.IntegerType);
		
		Dataset<Row> groupMonthsDF = addedSelectDF.groupBy("SID").count().selectExpr("SID","getNoMonthCount(count) as noMonth");
		groupMonthsDF.show();
		
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
