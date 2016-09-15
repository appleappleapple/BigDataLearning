package com.lin.basecount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 功能概要：平均查询词长
 * 
 * @author linbingwen
 */
public class AverageLength {
	
	private static int a = 0;
	
	// map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {

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
				String c2 = tokenizerLine.nextToken();// 关键词
				InputSplit inputSplit = context.getInputSplit();
//
				String[] fileName = inputSplit.getLocations();
				a++;
				Text newline = new Text("20160731");
				context.write(newline, new DoubleWritable(c2.length()));
			}

		}
        
	}
	
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		// 实现reduce函数
        @Override
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
			int count = 0;
			 for(DoubleWritable val:values){
				  count += val.get();
			 }
			double result = (double)count/(double)a;
			context.write(key, new DoubleWritable(result));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//设置hadoop的机器、端口
		conf.set("mapred.job.tracker", "10.75.201.125:9000");
		//设置输入输出文件目录
		String[] ioArgs = new String[] { "hdfs://hmaster:9000/cleanSame_out", "hdfs://hmaster:9000/new_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		//设置一个job
		Job job = Job.getInstance(conf, "key Word count");
		job.setJarByClass(AverageLength.class);
		
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(AverageLength.Map.class);
		job.setCombinerClass(AverageLength.Reduce.class);
		job.setReducerClass(AverageLength.Reduce.class);
		
		// 设置输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
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
