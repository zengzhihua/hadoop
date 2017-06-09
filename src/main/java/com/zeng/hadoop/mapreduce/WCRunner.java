package com.zeng.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCRunner {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job wcjob = Job.getInstance(conf);
		
		wcjob.setJarByClass(WCRunner.class);
		
		wcjob.setMapperClass(WCMapper.class);
		wcjob.setReducerClass(WCReducer.class);
		
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(wcjob, new Path("F:/input/"));
		
		FileOutputFormat.setOutputPath(wcjob, new Path("F:/output/"));
		
		wcjob.waitForCompletion(true);
	}
	
	
	
	
}
