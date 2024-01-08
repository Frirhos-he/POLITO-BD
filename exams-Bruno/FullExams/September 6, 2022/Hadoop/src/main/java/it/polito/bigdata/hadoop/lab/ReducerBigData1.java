package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                kwh2021,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    /* 
    protected void setup(Context context) {
                    }
    */             
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<kwh2021> values, // Input value type
        Context context) throws IOException, InterruptedException {

        for (kwh2021 value : values) {
			
            if(value.count21> value.count20)
			    context.write(new Text(key), new NullWritable()); 
		}
    }
    /*
    protected void cleanup(Context context) throws IOException, InterruptedException {
		// emit the local top K list
		for (WordCountWritable p : localTopK.getLocalTopK()) {
			context.write(new Text(p.getWord()), new IntWritable(p.getCount()));
		}
	}
     */
}
