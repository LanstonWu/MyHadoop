package com.sywu.myhadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) {
	

		if(args.length!=2){
			System.err.print("参数传入错误!使用示例: WordCount <输入路径> <结果输出路径>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "WordCount");
			job.setJarByClass(WordCount.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(SumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}//:end
