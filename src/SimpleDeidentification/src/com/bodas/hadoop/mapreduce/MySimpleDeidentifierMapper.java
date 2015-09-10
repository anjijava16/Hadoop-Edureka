package com.bodas.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MySimpleDeidentifierMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		//0, 63891 20130101  5.102  -86.61   32.85    12.8     9.6    11.2    11.6 
		
		String line = value.toString().trim();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
	   
		String result = "";
		int tokenNo = 0;
		
		while(tokenizer.hasMoreTokens()) {
			
			String token = tokenizer.nextToken();
			
			switch(tokenNo){
				case 3:
					result = result + "MMM-MMM-MMMM" + ",";
					break;
				case 5:
					result = result + "SSN-SS-SSSS" + ",";
					break;
				default :
					result = result + token + ",";
					break;
			}
			
			tokenNo++;			
		}
	    
	    result = result.substring(0,result.length()-1);
		    
		context.write(NullWritable.get(), new Text(result));
		    
	    
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


