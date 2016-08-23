package com.lin.counttime;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 功能概要：
 * 
 * @author linbingwen
 * @since  2016年8月1日 
 */
public class CountTime {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		// 实现map函数
        @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 将输入的纯文本文件的数据转化成String
			String line = value.toString();
			
			// 将输入的数据首先按行进行分割
			StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
			// 分别对每一行进行处理
			while (tokenizerArticle.hasMoreElements()) {
			    // 每行按空格划分
			    StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
			    String c1 = tokenizerLine.nextToken();//
			    Text newline = new Text(c1);
			    context.write(newline, one);
			}
		}

	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result =new IntWritable();
        
		// 实现reduce函数
        @Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int count = 0;
			 for(IntWritable val:values){
				  count += val.get();
			 }
	        result.set(count);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//设置hadoop的机器、端口
		conf.set("mapred.job.tracker", "10.75.201.125:9000");
		//设置输入输出文件目录
		String[] ioArgs = new String[] { "hdfs://hmaster:9000/one_in", "hdfs://hmaster:9000/one_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		//设置一个job
		Job job = Job.getInstance(conf, "key Word count");
		job.setJarByClass(CountTime.class);
		
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(CountTime.Map.class);
		job.setCombinerClass(CountTime.Reduce.class);
		job.setReducerClass(CountTime.Reduce.class);
		
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
