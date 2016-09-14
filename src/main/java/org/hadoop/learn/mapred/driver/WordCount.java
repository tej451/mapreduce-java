package org.hadoop.learn.mapred.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.learn.mapred.mapper.WordCountMapper;
import org.hadoop.learn.mapred.reducer.WordCountReducer;

public class WordCount extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		// creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("WordCount");
		conf.set("fs.default.name", "hdfs://" + "hadoopmaster" + ":9000");
		conf.setJarByClass(getClass());

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);

		// the hdfs input and output directory to be fetched from the command
		// line
		//FileInputFormat.addInputPath(conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		FileInputFormat.addInputPath(conf, new Path("/bookshelf/textbook.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/bookshelf/out"));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
}
