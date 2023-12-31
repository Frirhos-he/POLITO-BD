package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                YearCounts,  // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<YearCounts> values, // Input value type
        Context context) throws IOException, InterruptedException {

            System.out.println("here4");
        String SID = key.toString();
        System.out.println(SID);
        // Iterate over the set of values 
        for (YearCounts value : values) {
        	if (value.getCount2018()>30 && value.getCount2019()>30)
        		context.write(new Text(SID), null);
        }
    }
}
