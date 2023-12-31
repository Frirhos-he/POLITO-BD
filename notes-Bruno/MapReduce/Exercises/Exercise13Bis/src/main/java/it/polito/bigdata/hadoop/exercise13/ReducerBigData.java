package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,  // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    	
	
	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers as the 
	// same key (NullWritable.get())
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float dailyIncome;
    	String date;
		
    	DateValue top1;
    	DateValue top2;
    	
    	top1=null;
    	top2=null;
    	
        // Iterate over the set of values and select the top 2
        for (Text value : values) {

        	String[] record=value.toString().split("_"); 
    		
        	date=record[0];
        	dailyIncome=Float.parseFloat(record[1]);

        	if (top1==null || top1.value<dailyIncome)
    		{
    			top2=top1;
    			
    			top1=new DateValue();
    			top1.date=new String(date);
    			top1.value=dailyIncome;
    		}
    		else
    			{
    				if (top2==null || top2.value<dailyIncome)
    				{
    					top2=new DateValue();
    					top2.date=new String(key.toString());
    					top2.value=dailyIncome;
    				}
    			}
        }

        // Emit pair (date, value) top1
        // Emit pair (date, value) top2
        context.write(new Text(top1.date), new FloatWritable(top1.value));
        context.write(new Text(top2.date), new FloatWritable(top2.value));
    }
}
