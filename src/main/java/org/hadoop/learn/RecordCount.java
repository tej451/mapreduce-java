package org.hadoop.learn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class RecordCount extends Configured implements Tool {

	private static class RecordMapper 
	extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable lineOffset, Text record, Context output) 
		throws IOException, InterruptedException {
			output.write(new Text("It"), new LongWritable(1));
		}

	}

	public int run(String[] arg0) throws Exception {
		
		 Configuration conf = new Configuration();
		 conf.set("fs.default.name", "hdfs://" + "hadoopmaster" + ":9000");
		 //conf.set("mapred.job.tracker", "hadoopmaster" + ":54311");
		 
		
		Job job = Job.getInstance(conf);
		
		
		
//		job.setJar("nyse-0.0.1-SNAPSHOT.jar");
		job.setJarByClass(getClass());
		
		job.setMapperClass(RecordMapper.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(LongSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/bookshelf/textbook.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/bookshelf/out"));
		 
				
			int b =	job.waitForCompletion(true) ? 0 : 1;
				
				return b;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new RecordCount(), args));
	}

}