package com.ssy.prefix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {


    public static final IntWritable one = new IntWritable(1);
    private Text appKey = new Text();
    private String appStr;

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        appStr = value.toString().split(",")[1];
        if ( "91988061".equals( appStr ) ) {
            appKey.set( appStr + (int)(MyPartitioner.WEILI_ANDROID_REDUCE_NUM*Math.random()) );
        } else if ( "99817749".equals( appStr ) ) {
            appKey.set( appStr + (int)(MyPartitioner.ZHWNL_ANDROID_REDUCE_NUM*Math.random()) );
        } else if ( "91988062".equals( appStr ) ) {
            appKey.set( appStr + (int)(MyPartitioner.WEILI_IOS_REDUCE_NUM*Math.random()) );
        } else {
            appKey.set( appStr + (int)(MyPartitioner.ZHWNL_IOS_REDUCE_NUM*Math.random()) );
        }
        context.write(appKey,one);
    }
}
