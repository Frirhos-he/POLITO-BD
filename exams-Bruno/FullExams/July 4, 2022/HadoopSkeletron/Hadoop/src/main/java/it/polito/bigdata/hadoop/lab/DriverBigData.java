package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    
    Path inputPath;
    Path outputDir;
    Path outputDir1;
    int numberOfReducersJob1;
    int numberOfReducersJob2;
    int exitCode;

    numberOfReducersJob1 = Integer.parseInt(args[0]);
    numberOfReducersJob2 = Integer.parseInt(args[1]);
    inputPath = new Path(args[2]);
    outputDir = new Path("outJob1");
    outputDir1 = new Path("outJob2");

    Configuration conf = this.getConf();

    Job job = Job.getInstance(conf);
    job.setJobName("Exam");
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.setJarByClass(DriverBigData.class);
    job.setInputFormatClass(TextInputFormat.class); //KeyValueTextInputFormat
    job.setOutputFormatClass(TextOutputFormat.class); 
    job.setMapperClass(MapperBigData1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(ReducerBigData1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(numberOfReducersJob1);
    if (job.waitForCompletion(true) == true) {
      Job job2 = Job.getInstance(conf);

      job2.setJobName("Exam1");
      FileInputFormat.addInputPath(job2, outputDir);
      FileOutputFormat.setOutputPath(job2, outputDir1);
      job2.setJarByClass(DriverBigData.class);
      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      job2.setMapperClass(MapperBigData2.class);
      job2.setMapOutputKeyClass(NullWritable.class);
      job2.setMapOutputValueClass(OperatinPached.class);
      job2.setReducerClass(ReducerBigData2.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(IntWritable.class);
      job2.setNumReduceTasks(numberOfReducersJob2);

      if (job2.waitForCompletion(true)==true)
      exitCode=0;
     else
       exitCode=1;
}
else
  exitCode=1;
  
return exitCode;
}

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
    System.exit(res);
  }
}

      
