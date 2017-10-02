package com.sywu.hadoop.main;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;
import com.sywu.hadoop.mapper.TemperatureMapper;
import com.sywu.hadoop.reduce.TemperatureReduce;

public class TemperatureMain {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.print("参数传入错误!使用示例: WordCount <输入路径> <结果输出路径>");
			System.exit(-1);
		}

		JobConf jobConf = new JobConf();
		jobConf.setJobName("TemperatureMapperReduce");
		jobConf.setJarByClass(TemperatureMain.class);
		jobConf.setMapperClass(TemperatureMapper.class);
		jobConf.setReducerClass(TemperatureReduce.class);
		// 设置输入路径
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		// 设置输出路径
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		// 设置键输出格式
		jobConf.setOutputKeyClass(Text.class);
		// 设置键值输出格式
		jobConf.setOutputValueClass(DoubleWritable.class);
		try {
			JobClient.runJob(jobConf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
