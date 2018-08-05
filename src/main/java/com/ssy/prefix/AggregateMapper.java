package com.ssy.prefix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AggregateMapper extends Mapper<Object, Text, Text, IntWritable> {


    public static final IntWritable count = new IntWritable(1);
    private Text appKey = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        appKey.set( split[0] );
        count.set( Integer.parseInt(split[1]) );
        context.write(appKey,count);
    }
}
