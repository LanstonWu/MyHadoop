package com.sywu.hadoop.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/***
 * 分析最高温度Mapper类
 * @author lanstonwu
 *
 */
public class TemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	static enum MyCounters {
		NUM_RECORDS
	}
	private final double MISSING = 999.9;


	private String mapTaskId;
	private String inputFile;
	private int noRecords = 0;
	// 获取作业信息
	public void configure(JobConf job) {
		mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
		inputFile = job.get(JobContext.MAP_INPUT_FILE);
	}

	public void map(LongWritable offset, Text input, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String line = input.toString();//将输入转换为字符
		String yearStr = line.substring(14, 20), //截取年月字符
				tempStr = line.substring(25,30); // 截取温度字符
		double maxTemp = 0;
		
		++noRecords;
		// Increment counters
		reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

		// 更新作业状态信息
		if ((noRecords % 100) == 0) {
			reporter.setStatus(mapTaskId + " processed " + noRecords + " from input-file: " + inputFile);
		}
		if (!tempStr.matches("^([^A-Za-z]*?[A-Z][A-Za-z]*?)+.?")) {//匹配非字符情况时进行下面的操作
			maxTemp = Double.parseDouble(tempStr);
			if (maxTemp != MISSING)
				output.collect(new Text(yearStr), new DoubleWritable(maxTemp));
		}
	}
}