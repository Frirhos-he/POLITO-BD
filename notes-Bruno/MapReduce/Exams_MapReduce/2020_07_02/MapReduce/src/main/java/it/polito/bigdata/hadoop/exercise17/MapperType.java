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

HashMap<String, YearCounts> serverCounts;
protected void setup(Context context) {
		serverCounts = new HashMap<String, YearCounts>();
	}
	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		//PATCHED SERVER
	
		String record = value.toString();
		// Split each record by using the field separator
		// fields[0]= SID
		// fields[1]= PID
		// fields[2]= Date

		String[] fields = record.split(",");

		String SID = fields[0];
		String Date = fields[2];

		YearCounts tmp;

		if(Date.startsWith("2018") || Date.startsWith("2017")){
			if(serverCounts.get(SID) == null) {
				tmp = new YearCounts(0, 0);
				if(Date.startsWith("2018"))
					tmp.setCount2018(1);
				else 	tmp.setCount2019(1);
				serverCounts.put(SID,tmp);
			} else {
				if(Date.startsWith("2018")){
					serverCounts.get(SID).setCount2018(serverCounts.get(SID).getCount2018()+1);
				} else {
					serverCounts.get(SID).setCount2019(serverCounts.get(SID).getCount2019()+1);
				}
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, YearCounts> pair : serverCounts.entrySet()) {
			System.out.println(pair.getKey()+" "+pair.getValue().getCount2018()+" "+pair.getValue().getCount2019());
			context.write(new Text(pair.getKey()),pair.getValue());
		}
	}
}
