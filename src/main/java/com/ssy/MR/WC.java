package com.ssy.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
//        conf.setLong(FileInputFormat.SPLIT_MAXSIZE,67108464);   //64M
//        conf.setLong(FileInputFormat.SPLIT_MINSIZE,268433856); //256M
//        FileInputFormat.setMaxInputSplitSize(job, size);
//        FileInputFormat.setMinInputSplitSize(job, size);
//        conf.set(MRJobConfig.IO_SORT_MB,"100");
//        conf.set(MRJobConfig.MAP_SORT_SPILL_PERCENT,"0.8");
        System.setProperty("HADOOP_USER_NAME", "root");

        Job  job = Job.getInstance(conf); //job  作业
        job.setJarByClass(WC.class);//参数为 当前类的 类名
        job.setJobName("myjob");//给当前Job 取个名字
        job.setNumReduceTasks(4);
//        job.setCombinerClass( MyReducer.class );
//        job.setNumReduceTasks(1);

        Path input = new Path("/user/hive/warehouse/test.db/tmp_ods_hive_pv_1d");//输入路径
        FileInputFormat.addInputPath( job, input );
        Path output = new Path("/tmp/MR/pv");   //输出路径
        if( output.getFileSystem(conf).exists(output) ) {       //如果路径存在,删除路径
            output.getFileSystem(conf).delete(output,true);
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
