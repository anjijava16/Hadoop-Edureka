package com.bodas.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		//0, 1900 23
		//8, 1900 34
		if (tokenizer.hasMoreTokens()) {
			context.write(new Text(tokenizer.nextToken()), new IntWritable(new Integer(tokenizer.nextToken()).intValue()));
		}
		
		
	}
}
