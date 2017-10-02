package com.sywu.myhadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	IntWritable numOfeachParse=new IntWritable(1);
	Text word=new Text();
	
	@Override
	/***
	 * key: integer offset值
	 * value: 输入文件的内容
	 * context: 传输给reduce的内容
	 */
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer stz=new StringTokenizer(value.toString());
		while (stz.hasMoreElements()) {
			word.set(stz.nextToken());
			//只统计字符数,每一个字符赋值为1,写入格式<a,1> <a,1,1>
			context.write(word,numOfeachParse);
		}
	}
}