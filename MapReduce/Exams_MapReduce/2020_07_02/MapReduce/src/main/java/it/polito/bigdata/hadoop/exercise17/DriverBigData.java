package it.polito.bigdata.hadoop.exercise17;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Driver class.
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath1;
    Path outputDir;
    int numberOfReducers;
	  int exitCode;  
	
	// Parse the parameters
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath1 = new Path(args[1]);  
    outputDir = new Path(args[2]);
    System.out.println(numberOfReducers+" "+inputPath1+ " "+ outputDir);
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Exam_2020_07_02");


    FileInputFormat.addInputPath(job, inputPath1);
       
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);

    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);

    job.setInputFormatClass(TextInputFormat.class);
    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);

        
    job.setMapperClass(MapperType.class);
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(YearCounts.class);
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
    System.out.println("here1");
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    System.out.println("here2");
    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers);
        System.out.println("here3");
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
    	
    return exitCode;
  }

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}