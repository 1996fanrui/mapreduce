package com.ssy.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends  Reducer<TQ, IntWritable, Text, IntWritable>{
	
	
	Text rkey = new Text();
	IntWritable rval = new IntWritable();
	
	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int flg=0;
		int day=0;
		for (IntWritable val : values) {
			if(flg==0){
				
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd());
				context.write(rkey, rval);
				day=key.getDay();
				flg++;
			}
			
			if(flg != 0  &&  day != key.getDay()){
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd());
				context.write(rkey, rval);
				break;
			}
				
			
		}
		
		
		
		
	}

}
