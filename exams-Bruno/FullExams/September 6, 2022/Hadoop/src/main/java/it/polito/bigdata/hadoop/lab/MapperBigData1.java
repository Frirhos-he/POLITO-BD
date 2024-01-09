package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */



/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    kwh2021> {// Output value type
    
    HashMap<String,kwh2021> dataCounter;

      	protected void setup(Context context) {
		dataCounter = new HashMap<String, kwh2021>();
	}


    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            
            String [] words = value.toString().split(",");
            // CodDC,Date,kWh
            for(String word: words){
                String[] fields = word.split(",");
                if(Long.parseLong(fields[2])> 1000){
                    kwh2021 tmp;
                    if(fields[1].startsWith("2020") == true){
                        if(dataCounter.get(fields[0]) != null){
                        
                        tmp = dataCounter.get(fields[0]);
                        tmp.count20 = tmp.count20+1;

                        dataCounter.put(fields[0],tmp);
                        }else{
                            tmp = new kwh2021(1,0);
                            dataCounter.put(fields[0],tmp);
                        }
                    } else if(fields[1].startsWith("2021") == true) {
                        if(dataCounter.get(fields[0]) != null){    
                            tmp = dataCounter.get(fields[0]);
                            tmp.count21 = tmp.count21+1;
                            dataCounter.put(fields[0],tmp);
                        }else{
                            tmp = new kwh2021(0,1);
                            dataCounter.put(fields[0],tmp);
                        }
                    }

                }
            }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

		// Emit the set of (key, value) pairs of this mapper
		for (Entry<String, kwh2021> pair : dataCounter.entrySet()) {
			context.write(new Text(pair.getKey()),
					pair.getValue());
		}

	} 
}
