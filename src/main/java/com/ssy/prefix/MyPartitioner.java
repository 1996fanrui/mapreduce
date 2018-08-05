package com.ssy.prefix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,IntWritable> {

    public static final int WEILI_ANDROID_REDUCE_NUM = 9;
    public static final int ZHWNL_ANDROID_REDUCE_NUM = 9;
    public static final int WEILI_IOS_REDUCE_NUM = 2;
    public static final int ZHWNL_IOS_REDUCE_NUM = 2;
    public static final int REDUCE_NUM = WEILI_ANDROID_REDUCE_NUM + ZHWNL_ANDROID_REDUCE_NUM
            + WEILI_IOS_REDUCE_NUM + ZHWNL_IOS_REDUCE_NUM ;

    String appStr;
    int offset;

    public int getPartition(Text key, IntWritable value, int numPartitions) {
        //91988061 ANDROID
        //中华万年历Android	99817749
        //91988062 IOS
        //中华万年历iPhone	99817882
        offset = Integer.parseInt( key.toString().substring(8) );
        appStr = key.toString().substring(0,8);
        if( "91988061".equals( appStr ) ) { //微鲤看看安卓
            return offset;
        } else if ( "99817749".equals( appStr ) ) {//万年历安卓
            return WEILI_ANDROID_REDUCE_NUM + offset;
        } else if ( "91988062".equals( appStr ) ) {
            return WEILI_ANDROID_REDUCE_NUM + ZHWNL_ANDROID_REDUCE_NUM
                    + offset;
        } else {
            return WEILI_ANDROID_REDUCE_NUM + ZHWNL_ANDROID_REDUCE_NUM
                    + WEILI_IOS_REDUCE_NUM + offset;
        }
    }
}
