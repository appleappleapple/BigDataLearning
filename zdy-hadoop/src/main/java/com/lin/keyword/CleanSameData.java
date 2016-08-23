package com.lin.keyword;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * 功能概要：数据去重
 * 
 * @author linbingwen
 * @since 2016年7月31日
 */
public class CleanSameData {
	// map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, Text> {

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
				c2 = c2.substring(1, c2.length() - 1);
				Text newline = new Text(c1 + "    " + c2);
				context.write(newline, new Text(""));
			}

		}

	}

	// reduce将输入中的key复制到输出数据的key上，并直接输出
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
        @Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// 设置hadoop的机器、端口
		conf.set("mapred.job.tracker", "10.75.201.125:9000");
		// 设置输入输出文件目录
		String[] ioArgs = new String[] { "hdfs://hmaster:9000/data_in", "hdfs://hmaster:9000/data_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		// 设置一个job
		Job job = Job.getInstance(conf, "clean same data");
		job.setJarByClass(CleanSameData.class);

		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
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
