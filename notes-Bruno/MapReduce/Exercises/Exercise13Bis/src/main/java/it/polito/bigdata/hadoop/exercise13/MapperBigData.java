package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> {// Output value type

	DateValue top1;
	DateValue top2;
	

	protected void setup(Context context) {
		top1 = null;
		top2 = null;
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String date = new String(key.toString());
		float dailyIncome = Float.parseFloat(value.toString());

		if (top1 == null || top1.value < dailyIncome || (top1.value == dailyIncome && date.compareTo(top1.date) < 0)) {
			top2 = top1;

			top1 = new DateValue();
			top1.date = date;
			top1.value = dailyIncome;
		} else {
			if (top2 == null || top2.value < dailyIncome
					|| (top2.value == dailyIncome && date.compareTo(top2.date) < 0)) {
				top2 = new DateValue();
				top2.date = date;
				top2.value = dailyIncome;
			}
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text(top1.date + "_" + top1.value));
		context.write(NullWritable.get(), new Text(top2.date + "_" + top2.value));
	}

}
