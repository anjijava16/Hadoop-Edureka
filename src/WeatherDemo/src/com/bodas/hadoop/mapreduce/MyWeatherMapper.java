package com.bodas.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyWeatherMapper extends Mapper<LongWritable, Text, Text, Text>{

	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		//0, 63891 20130101  5.102  -86.61   32.85    12.8     9.6    11.2    11.6 
		
		String line = value.toString().trim();
		StringTokenizer tokenizer = new StringTokenizer(line);
	    String[] weatherArray = MapperHelper.getStringArray(tokenizer);
	    
	    if (weatherArray.length > 0) {
		    String keyTemp = weatherArray[1];
		    String maxTemp = weatherArray[5];
		    String minTemp = weatherArray[6];
		    
		    System.out.println("MaxTemp: " + maxTemp + "; MinTemp: " + minTemp);
		    
		    context.write(new Text(keyTemp), new Text(maxTemp + ", " + minTemp));
		    /*context.write(new Text(keyTemp), new FloatWritable(new Float(maxTemp).floatValue()));
		    context.write(new Text(keyTemp), new FloatWritable(new Float(minTemp).floatValue()));*/
			
		    //if (tokenizer.hasMoreTokens()) {
			//	context.write(new Text(tokenizer.nextToken()), new IntWritable(new Integer(tokenizer.nextToken()).intValue()));
			//}
			
	    }
	    
	    /*if (weatherArray.length > 0) {
	    	context.write(new Text(weatherArray[1] + "-2"), new FloatWritable(new Float(weatherArray[2]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-3"), new FloatWritable(new Float(weatherArray[3]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-4"), new FloatWritable(new Float(weatherArray[4]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-5"), new FloatWritable(new Float(weatherArray[5]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-6"), new FloatWritable(new Float(weatherArray[6]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-7"), new FloatWritable(new Float(weatherArray[7]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-8"), new FloatWritable(new Float(weatherArray[8]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-9"), new FloatWritable(new Float(weatherArray[9]).floatValue()));
	    	context.write(new Text(weatherArray[1] + "-10"), new FloatWritable(new Float(weatherArray[10]).floatValue()));
	    	
	    }*/
	}
	
	
}


