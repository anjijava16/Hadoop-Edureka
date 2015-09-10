package com.bodas.hadoop.mapreduce;

import java.util.StringTokenizer;

public class MapperHelper {

	public static String[] getStringArray(StringTokenizer tokenizer) {
	
		String[] result = new String[tokenizer.countTokens()];
		
		int x = 0;
		while (tokenizer.hasMoreElements()) {
			result[x] = tokenizer.nextElement().toString();                 
			x++;
		}

		return result;
	}
	
}
