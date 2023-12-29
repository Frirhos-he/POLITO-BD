package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.soap.Text;

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
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {


        String mid = key.toString();
        List<String> valueList = new ArrayList<>();
        for (Text value : values) {
            valueList.add(value.toString());
        }
        String []valueArray = valueList.toArray();
        int flag = 0;

        for(int i = 1; i < valueArray.length; i++){
            if(valueArray[0].compareTo(valueArray[i])!=0){
                flag = 1;
            }
        }
        if(flag == 0) context.write(new Text(mid),NullWritable.get());
    }
}
