package com.ssy.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner  extends  Partitioner<TQ, IntWritable> {

	@Override
	public int getPartition(TQ key, IntWritable value, int numPartitions) {
		return key.getYear() % numPartitions;	//按照 年 进行分区
	}

}
