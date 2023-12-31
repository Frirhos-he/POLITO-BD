package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second data format
 */
class MapperType extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		YearCounts> {// Output value type

HashMap<String, Integer> userfilmMap;
protected void setup(Context context) {
		userfilmMap = new HashMap<String, Integer>();
	}
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {


	
		String record = value.toString();

		String[] fields = record.split(",");
		String startTimeStamp = fields[2];
		String username = fields[0];
		String MID = fields[1];

		if(startTimeStamp.startsWith("2019")){
			if(userfilmMap.get(MID).compareTo(username)==false)
						userfilmMap.put(MID,username);//remove duplicate of the user watching the same movie
		}
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(MID), new Text(username));
	}
}
