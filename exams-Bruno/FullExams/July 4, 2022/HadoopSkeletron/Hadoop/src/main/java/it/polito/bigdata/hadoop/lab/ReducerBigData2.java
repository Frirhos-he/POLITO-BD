package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData2 extends
    Reducer<NullWritable,
        OperatinPached,
            Text,
            IntWritable> {

    private Integer k = 100;

    @Override
    protected void reduce(NullWritable key,
                          Iterable<OperatinPached> values,
                          Context context) throws IOException, InterruptedException {


        OperatinPached globalTop = new OperatinPached();
        
       for (OperatinPached currentTop : values) {
        System.out.println(currentTop.count);
        if(currentTop.count>=globalTop.count){

            if(currentTop.count>globalTop.count){
                globalTop.operatingSystem = currentTop.operatingSystem;
                       System.out.println(globalTop.operatingSystem);
                globalTop.count = currentTop.count;
            }
            if(currentTop.count == globalTop.count){
                if(key.toString().compareTo(globalTop.operatingSystem)<0){ //alphabetical order
                    globalTop.operatingSystem = currentTop.operatingSystem;
                }   
            }
        }
        }

        context.write(new Text(globalTop.operatingSystem), new IntWritable(globalTop.count));
    }
}
