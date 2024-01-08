package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData2 extends
    Reducer<NullWritable,
            DoubleWritable,
            Text,
            IntWritable> {

    private Integer k = 100;

    @Override
    protected void reduce(NullWritable key,
                          Iterable<DoubleWritable> values,
                          Context context) throws IOException, InterruptedException {

     //   TopKVector<WordCountWritable> globalTopK = new TopKVector<WordCountWritable>(k);

    //    for (WordCountWritable currentPair : values) {
    //        globalTopK.updateWithNewElement(new WordCountWritable(currentPair));
   //     }

    //    for (WordCountWritable p : globalTopK.getLocalTopK()) {
   //         context.write(new Text(p.getWord()), new IntWritable(p.getCount()));
   //     }

    }
}
