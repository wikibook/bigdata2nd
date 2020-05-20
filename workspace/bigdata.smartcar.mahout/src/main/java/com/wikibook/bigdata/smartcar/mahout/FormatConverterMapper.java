package com.wikibook.bigdata.smartcar.mahout;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class FormatConverterMapper extends Mapper<LongWritable,Text,Text,Text> {
         
         
        @Override
        protected void map(LongWritable key, Text value,Context context)  throws IOException, InterruptedException {
        	
        		String[] splitData = value.toString().split(" ");
        		
        		String vData = "";
        		
        		for(int i=1; i < splitData.length; i++) {
        			if (i == 1) vData += splitData[i];
        			else vData += " " + splitData[i];
        		}
        		
                context.write(new Text(splitData[0]), new Text(vData));                
        }
 
}