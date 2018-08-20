package com.ssy.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTQ {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MyTQ.class);
		
		
		//input,output
		
		Path input = new Path("/mr/tq/input");
		FileInputFormat.addInputPath(job, input );
		
//		设置文件的切片大小
//		FileInputFormat.setMaxInputSplitSize(job, size);
//		FileInputFormat.setMinInputSplitSize(job, size);
		
		Path output = new Path("/mr/tq/output");
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output );
		
		//mapTask
		job.setMapperClass(TMapper.class);
		job.setMapOutputKeyClass(TQ.class);		//自定义的天气对象
		job.setMapOutputValueClass(IntWritable.class);	//温度
		//自定义分区器（计算P）
		job.setPartitionerClass(TPartitioner.class);

		//重写分组比较器（分到同一组的数据调用一次reduce方法）
		job.setGroupingComparatorClass(TGroupComparator.class);

		// 重写Map端的排序比较器（内存缓冲区溢写磁盘时的排序比较器）
		// 自定义的比较器，要根据Reduce端的分组比较器而设计（尽量与Reduce端需要的数据顺序一致）
		//  一般封装的key的比较器都是适合大多数情况的。
		job.setSortComparatorClass(TSorter.class);

		job.setReducerClass(TReducer.class);
		
		job.setNumReduceTasks(2);	//Reduce的数量
		
		
		//submit
		job.waitForCompletion(true);
	}
	
	
	

}
