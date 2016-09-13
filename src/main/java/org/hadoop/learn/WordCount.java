package org.hadoop.learn;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		/*
		 * To run in Windows machine one need to add the hadoop windows binaries
		 * to windows PATH as described below link
		 * https://github.com/karthikj1/Hadoop-2.7.1-Windows-64-binaries
		 */
		// System.setProperty("hadoop.home.dir",
		// "E:\\hadoop\\hadoop-common-2.2.0-bin");
		Configuration conf = new Configuration();
		// run on other machine/cluster
		conf.set("fs.default.name", "hdfs://" + "hadoopmaster" + ":9000");

		// conf.set("hbase.zookeeper.quorum", "hadoopmaster");
		// conf.set("hbase.zookeeper.property.clientPort", "2181");
		// conf.set("fs.default.name","hdfs://hadoopmaster:9000");
		// conf.set("mapred.job.tracker", "hdfs://hadoopmaster:54311");

		/*
		 * if (args.length != 2) { System.err.println("Usage: <in> <out>");
		 * System.exit(2); } FileInputFormat.addInputPath(job, new
		 * Path(args[0])); FileOutputFormat.setOutputPath(job, new
		 * Path(args[1]));
		 */

		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//HDFS i/p & o/p file paths: Output directory should not exist in HDFS
		FileInputFormat.addInputPath(job, new Path("/bookshelf/textbook.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/bookshelf/out"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
