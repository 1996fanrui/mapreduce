package com.ssy.prefix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WC {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);//读取配置文件
        conf.set("mapred.jar", "E:\\code\\ssy\\mapreduce\\out\\artifacts\\MyWC\\MyWC.jar");
        conf.set("fs.defaultFS", "hdfs://nn1.hadoop.wnl.dmp.com:8020");
//        conf.setLong(FileInputFormat.SPLIT_MAXSIZE,67108464);   //64M
//        conf.setLong(FileInputFormat.SPLIT_MINSIZE,268433856); //256M
//        conf.setInt(MRJobConfig.NUM_MAPS,400);
        System.setProperty("HADOOP_USER_NAME", "root");
        Job job = Job.getInstance(conf); //job  作业
        job.setJarByClass(WC.class);//参数为 当前类的 类名
        job.setJobName("prefix");//给当前Job 取个名字
        job.setNumReduceTasks(MyPartitioner.REDUCE_NUM);

        Path input = new Path("/user/hive/warehouse/test.db/tmp_ods_hive_pv_1d");//输入路径
        FileInputFormat.addInputPath( job, input );
        FileSystem fileSystem = input.getFileSystem(conf);
        Path output = new Path( "/tmp/MR/prefix" );   //输出路径
        if( fileSystem.exists(output) ) {       //如果路径存在,删除路径
            fileSystem.delete(output,true);
        }
        FileOutputFormat.setOutputPath( job, output );//为job设置输出路径

        job.setMapperClass(MyMapper.class);    //设置map的类
        job.setMapOutputKeyClass(Text.class);    //map的key输出类型
        job.setMapOutputValueClass(IntWritable.class);//map的value输出类型

        job.setReducerClass(MyReducer.class);    //设置reduce的类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(MyPartitioner.class);   //设置分区器
        //将以上所有的代码 提交给集群,等待 完成

        job.waitForCompletion(true);


        Job job1 = Job.getInstance(conf); //job  作业
        job1.setJarByClass(WC.class);//参数为 当前类的 类名
        job1.setJobName("aggregate");//给当前Job 取个名字
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath( job1, output );
        Path output2 = new Path( "/tmp/MR/pv" );   //输出路径
        if( fileSystem.exists(output2) ) {       //如果路径存在,删除路径
            fileSystem.delete(output2,true);
        }
        FileOutputFormat.setOutputPath( job1, output2 );//为job设置输出路径

        job1.setMapperClass(AggregateMapper.class);    //设置map的类
        job1.setMapOutputKeyClass(Text.class);    //map的key输出类型
        job1.setMapOutputValueClass(IntWritable.class);//map的value输出类型
        job1.setReducerClass(AggregateReducer.class);    //设置reduce的类

        job1.waitForCompletion(true);

    }
}
