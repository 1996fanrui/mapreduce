package com.ssy.prefix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {


    public static final IntWritable one = new IntWritable(1);
    private Text appKey = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        appKey.set( value.toString().split(",")[1] );
        context.write(appKey,one);
    }

}
