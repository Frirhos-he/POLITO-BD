package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData1 extends Mapper<
    LongWritable,
    Text,
    Text,
    IntWritable> {
    
    HashMap<String, Integer> pidCounter;

    protected void setup(Context context) {
        pidCounter = new HashMap<String, Integer>();

    }

    protected void map(
        LongWritable key,
        Text value,
        Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        String date = words[1];

        if(date.compareTo("2021/07/04")>=0 && date.compareTo("2022/07/03") <= 0){
           if(pidCounter.get(words[2]) == null){
               pidCounter.put(words[2], 1);
           }else{
               pidCounter.put(words[2], pidCounter.get(words[2])+1);
           }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException { 
       for (Entry<String, Integer> pair : pidCounter.entrySet()) {
            context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
        }    
    }
}
