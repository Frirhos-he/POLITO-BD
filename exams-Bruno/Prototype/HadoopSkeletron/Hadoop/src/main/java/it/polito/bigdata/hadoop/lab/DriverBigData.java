package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBigData extends Configured implements Tool {
 /*  public static enum COUNTERS {
		SELECTED_WORDS, 
		DISCARDED_WORDS
	}
  */
  @Override
  public int run(String[] args) throws Exception {
    /*Path inputPath;
    Path outputDir;
    Path outputDir2;
    int numberOfReducersJob1;
    int numberOfReducersJob2;
    int exitCode;

    numberOfReducersJob1 = Integer.parseInt(args[0]);
    numberOfReducersJob2 = Integer.parseInt(args[1]);
    inputPath = new Path(args[2]);
    outputDir = new Path("outJob1");
    outputDir2 = new Path(args[3]);

    Configuration conf = this.getConf();
    conf.set("prefix", prefix);
	
    Job job = Job.getInstance(conf);
    job.setJobName("Lab 4 - Job 1");
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setJarByClass(DriverBigData.class);
    job.setInputFormatClass(TextInputFormat.class); //KeyValueTextInputFormat
    job.setOutputFormatClass(TextOutputFormat.class); //KeyValueTextOutputFormat
    job.setMapperClass(MapperBigData1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ProductIdRatingWritable.class);
    job.setReducerClass(ReducerBigData1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setNumReduceTasks(numberOfReducersJob1);

    if (job.waitForCompletion(true) == true) {
      Job job2 = Job.getInstance(conf);
      job2.setJobName("Lab 4 - Job 2");
      
      MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperType1BigData.class);
      MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperType2BigData.class);
     
      MultipleOutputs.addNamedOutput(job, 
      "hightemp", 
      TextOutputFormat.class, 
      FloatWritable.class, 
      NullWritable.class);
      MultipleOutputs.addNamedOutput(job, 
      "normaltemp", 
      TextOutputFormat.class, 
      Text.class, 
      NullWritable.class);
      FileInputFormat.addInputPath(job2, outputDir);
      FileOutputFormat.setOutputPath(job2, outputDir2);
      job2.setJarByClass(DriverBigData.class);
      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      job2.setMapperClass(MapperBigData2.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(DoubleWritable.class);
      job2.setReducerClass(ReducerBigData2.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(DoubleWritable.class);
      job2.setNumReduceTasks(numberOfReducersJob2);
      long Start = Time.now();
			job2.addCacheFile(
				new Path(outputDir + "/part-r-00000").toUri());
      if (job2.waitForCompletion(true) == true){
        Counter totRecords = job.getCounters().findCounter(MY_COUNTERS.TOTAL_RECORDS);
        exitCode = 0;
      }
      else
        exitCode = 1;
    } else
      exitCode = 1;

    return exitCode;
    */
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
    System.exit(res);
  }
}
