package com.lin.keyword;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * 功能概要：统计top 热搜词
 * 
 * @author linbingwen
 */
public class TopK {

	public static final int K = 100;
	
	public static class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		TreeMap<Integer, String> map = new TreeMap<Integer, String>(); 
		
        @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if(line.trim().length() > 0 && line.indexOf("\t") != -1) {
				String[] arr = line.split("\t", 2);
				String name = arr[0];
				Integer num = Integer.parseInt(arr[1]);
				map.put(num, name);
				if(map.size() > K) {
					map.remove(map.firstKey());
				}
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			for(Integer num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
		}
	}
	
	
	public static class KReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		TreeMap<Integer, String> map = new TreeMap<Integer, String>();
		
        @Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			map.put(key.get(), values.iterator().next().toString());
			if(map.size() > K) {
				map.remove(map.firstKey());
			}
		}

		@Override
		protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			for(Integer num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//设置hadoop的机器、端口
		conf.set("mapred.job.tracker", "10.75.201.125:9000");
		//设置输入输出文件目录
		String[] ioArgs = new String[] { "hdfs://hmaster:9000/top_in", "hdfs://hmaster:9000/top_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		//设置一个job
		Job job = Job.getInstance(conf, "top K");
		
        job.setJarByClass(TopK.class);
		
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(KMap.class);
		job.setCombinerClass(KReduce.class);
		job.setReducerClass(KReduce.class);
		
		// 设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
		job.setInputFormatClass(TextInputFormat.class);
		
		// 提供一个RecordWriter的实现，负责数据输出
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
