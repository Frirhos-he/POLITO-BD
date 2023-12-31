package it.polito.bigdata.hadoop.exercise14;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 14 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		NullWritable, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(javax.xml.soap.Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int count=0;
		for(IntWritable value: values){
			count++;
		}
		String []fields = key.toString().split("-");
		String Customerid = fields[0];
		String BID = fields[1];

		if(count > 2) context.write(new Text(Customerid),new Text(BID));

	}
}
