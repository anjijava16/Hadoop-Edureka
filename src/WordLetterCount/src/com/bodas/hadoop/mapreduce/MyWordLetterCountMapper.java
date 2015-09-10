package com.bodas.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyWordLetterCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		//0, Hadoop is great
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			context.write(new Text(String.valueOf(token.length())), new IntWritable(1));
		}
		
		
	}
}
