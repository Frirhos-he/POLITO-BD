package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData2 extends Mapper<
    Text,
    Text,
    NullWritable,
    OperatinPached> {
    
     OperatinPached tmp;
    protected void setup(Context context) {

        tmp = new OperatinPached();
    }

    protected void map(
        Text key,
        Text value,
        Context context) throws IOException, InterruptedException {
        if(Integer.parseInt(value.toString())>=tmp.count){

            if(Integer.parseInt(value.toString())>tmp.count){
                tmp.operatingSystem = key.toString();
                        System.out.println(tmp.operatingSystem);
                tmp.count = Integer.parseInt(value.toString());
            }
            if(Integer.parseInt(value.toString()) == tmp.count){
                if(key.toString().compareTo(tmp.operatingSystem)<0){ //alphabetical order
                    tmp.operatingSystem = key.toString();
                }   
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), tmp);
    }
}
