package com.bodas.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyWeatherReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		/*int maxTemp=0;
		int minTemp= 100;
		
		for(IntWritable value : values){
			
			maxTemp = (value.get() > maxTemp) ? value.get() : maxTemp;
			minTemp = (value.get() < minTemp) ? value.get() : minTemp;	
		}
		
		context.write(key, new IntWritable(maxTemp));
		context.write(key, new IntWritable(minTemp));*/


	
	}
	

}
