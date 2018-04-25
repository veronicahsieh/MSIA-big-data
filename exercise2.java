import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

	//Text is the key of the MR, and intWritable is the value
    	private Text col_key = new Text();

	public void configure(JobConf job) {
	}


	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws 
	IOException {
		
		String line = value.toString();
        String[] line_list = line.split(",");
        if (line_list[line_list.length-1].equals("false")) {
        	
        	float num_four = Float.parseFloat(line_list[3]);
        	String col_comb = line_list[29] + "," + line_list[30] + "," + line_list[31] + "," + line_list[32];
        	
        	col_key.set(col_comb);
        	output.collect(col_key,new FloatWritable(num_four));
	    }
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<FloatWritable> values, 
		OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		float sum = 0;
		float count = 0;
  	  while (values.hasNext()) {
  		  count += 1;
  		  sum += values.next().get();
        }
  	  
  	  float col_key_avg = sum/count;
  	  output.collect(key, new FloatWritable(col_key_avg));
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise2.class);
	conf.setJobName("exercise2");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise2(), args);
	System.exit(res);
    }
}