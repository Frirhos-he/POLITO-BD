package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    /* 
    protected void setup(Context context) {
                    }
    */             
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        /*
          int numRatings = 0;
          double totRatings = 0;
          
          String productId = "";
          double rating;
          double avg;
          
          for (ProductIdRatingWritable productIdRating : values) {
              productId = productIdRating.getProductId();
              rating = productIdRating.getRating();
          
              productsRatings.put(productId, rating);
          
              numRatings++;
              totRatings = totRatings + rating;
          }
          
          avg = totRatings / (double) numRatings;
          
          for (Iterator<Entry<String, Double>> i = productsRatings.entrySet().iterator(); i.hasNext();) {
              Entry<String, Double> pair = i.next();
              productId = (String) pair.getKey();
              rating = (double) pair.getValue();
          
              context.write(new Text(productId), new DoubleWritable(rating - avg));
          }

          users=new HashSet<String>();
            //remove duplicate
            for (Text value : values) {
                users.add(value.toString());
            }
         */ 
    }
    /*
    protected void cleanup(Context context) throws IOException, InterruptedException {
		// emit the local top K list
		for (WordCountWritable p : localTopK.getLocalTopK()) {
			context.write(new Text(p.getWord()), new IntWritable(p.getCount()));
		}
	}
     */
}
