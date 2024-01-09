package it.polito.bigdata.hadoop.lab;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReducerBigData2 extends
    Reducer<NullWritable,
            YearIncome,
            Text,
            DoubleWritable> {

    @Override
    protected void reduce(NullWritable key,
                          Iterable<YearIncome> values,
                          Context context) throws IOException, InterruptedException {
        System.out.println("Reducer Started");
        YearIncome result = null;
        for (YearIncome currentPair : values) {
            System.out.println(currentPair.getYear());
            if(result == null){
                result = new YearIncome(currentPair.getYear(), currentPair.getIncome());
            } else {
                if(result.getIncome() < currentPair.getIncome()){
                    result = new YearIncome(currentPair.getYear(), currentPair.getIncome());
                } else if(result.getIncome() == currentPair.getIncome()){
                        if(Integer.parseInt(result.getYear()) > Integer.parseInt(currentPair.getYear())){
                            result = new YearIncome(currentPair.getYear(), currentPair.getIncome());
                        }
                }
            }
        }
        if(result != null) 
        context.write(new Text(result.getYear()), new DoubleWritable(result.getIncome()));
    }
}
