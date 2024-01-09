package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperBigData1 extends Mapper<
    LongWritable,
    Text,
    Text,
    kwh2021> {
    
  //  HashMap<String, kwh2021> dataCounter;
  //  String prefix;
    //private MultipleOutputs<Writable, NullWritable> mos = null;
  //  protected void setup(Context context) {
      //  dataCounter = new HashMap<String, kwh2021>();
    //    prefix = context.getConfiguration().get("prefix").toString();
      //  mos = new MultipleOutputs<Writable, NullWritable>(context);
//    }

    protected void map(
        LongWritable key,
        Text value,
        Context context) throws IOException, InterruptedException {
        /* 
        specifiedUser = context.getConfiguration().get("username");
        String[] words = value.toString().split(",");
        for (String word : words) {
            String[] fields = word.split(",");
            if (Long.parseLong(fields[2]) > 1000) {
                kwh2021 tmp;
                if (fields[1].startsWith("2020") == true) {
                    if (dataCounter.get(fields[0]) != null) {
                        tmp = dataCounter.get(fields[0]);
                        tmp.count20 = tmp.count20 + 1;
                        dataCounter.put(fields[0], tmp);
                    } else {
                        tmp = new kwh2021(1, 0);
                        dataCounter.put(fields[0], tmp);
                    }
                } else if (fields[1].startsWith("2021") == true) {
                    if (dataCounter.get(fields[0]) != null) {
                        tmp = dataCounter.get(fields[0]);
                        tmp.count21 = tmp.count21 + 1;
                        dataCounter.put(fields[0], tmp);
                    } else {
                        tmp = new kwh2021(0, 1);
                        dataCounter.put(fields[0], tmp);
                    }
                }
            }
        }
        String []words = value.toString().split("\\s+");

        ArrayList<String> listFiltered =  Arrays.stream(words).map(e-> e.toLowerCase()).filter(e -> e.startsWith(prefix)).collect(Collectors.toCollection(ArrayList::new));
        int countPrefixWords = listFiltered.size();
        int countNoPrefixWords = words.length - listFiltered.size();
        context.getCounter(COUNTERS.SELECTED_WORDS).increment(countPrefixWords);
        context.getCounter(COUNTERS.DISCARDED_WORDS).increment(countNoPrefixWords);
        for (String word : listFiltered){
            context.write(new Text(word), new IntWritable(1));
        }

        rules = new ArrayList<String>();
		URI[] CachedFiles = context.getCacheFiles();
		BufferedReader rulesFile = 
				new BufferedReader(
					new FileReader(new File(CachedFiles[0].getPath())));
		while ((nextLine = rulesFile.readLine()) != null) {
			rules.add(nextLine);
		}
		rulesFile.close();

        //multiple output
        if (temperature>30.0)
				mos.write("hightemp", 
						new FloatWritable(temperature), NullWritable.get());
			else
				mos.write("normaltemp", 
						value, NullWritable.get());
        */
    }

//    protected void cleanup(Context context) throws IOException, InterruptedException {
 //       for (Entry<String, kwh2021> pair : dataCounter.entrySet()) {
//            context.write(new Text(pair.getKey()), pair.getValue());
//        }
//    }
}
