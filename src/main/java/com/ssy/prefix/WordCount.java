package com.ssy.prefix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);//读取配置文件
        Job  job = Job.getInstance(conf); //job  作业
        job.setJarByClass(WordCount.class);//参数为 当前类的 类名
        job.setJobName("prefix");//给当前Job 取个名字
        Path input = new Path("/user/hive/warehouse/test.db/tmp_ods_hive_pv_1d/000000_0");//输入路径
        FileInputFormat.addInputPath( job, input );//为job添加输入路径
        Path output = new Path("/tmp/wordcount");//输出路径
        if( output.getFileSystem(conf).exists(output) ) {//如果路径存在
            output.getFileSystem(conf).delete(output,true);//删除路径
        }
        FileOutputFormat.setOutputPath( job, output );//为job设置输出路径
        job.setMapperClass(MyMapper.class);    //设置map的类
        job.setMapOutputKeyClass(Text.class);    //map的key输出类型
        job.setMapOutputValueClass(IntWritable.class);//map的value输出类型
        job.setReducerClass(MyReducer.class);    //设置reduce的类
        //将以上所有的代码 提交给集群,等待 完成

        job.waitForCompletion(true);

    }
}
