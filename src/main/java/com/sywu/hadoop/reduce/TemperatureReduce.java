package com.sywu.hadoop.reduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureReduce extends MapReduceBase implements Reducer<Text ,DoubleWritable, Text , DoubleWritable>{

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		double maxVal = 0; 
		while (values.hasNext()){
			 maxVal=Double.max(Double.MIN_VALUE,values.next().get());
		}
		output.collect(key,new DoubleWritable(maxVal));
	}
}