package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData1 extends Mapper<
    LongWritable,
    Text,
    Text,
    DoubleWritable> {
    
    HashMap<String, Double> yearIncome;
  //  String prefix;
    //private MultipleOutputs<Writable, NullWritable> mos = null;
    protected void setup(Context context) {
        yearIncome = new HashMap<String, Double>();
    }

    protected void map(
        LongWritable key,
        Text value,
        Context context) throws IOException, InterruptedException {
        
        String[] words = value.toString().split(",");
        String year = words[0].split("/")[0];
        Double salesPrice = Double.parseDouble(words[3]);
        
        if (yearIncome.get(year) != null) {
            yearIncome.put(year, yearIncome.get(year) + salesPrice);
        } else {
            yearIncome.put(year,salesPrice);
        }

    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Entry<String, Double> pair : yearIncome.entrySet()) {
           context.write(new Text(pair.getKey()), new DoubleWritable(pair.getValue()));
        }
    }
}
