package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData2 extends Mapper<
    Text,
    Text,
    NullWritable,
    YearIncome> {
    
    protected void map(
        Text key,
        Text value,
        Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),new YearIncome(key.toString(), Double.parseDouble(value.toString())));
        }
}
