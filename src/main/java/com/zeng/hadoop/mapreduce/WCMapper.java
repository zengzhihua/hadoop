package com.zeng.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 具体业务逻辑就写在这个方法体中，而且我们业务要处理的数据已经被框架传递进来，在方法的参数中 key-value
		// key 是这一行数据的起始偏移量 value 是这一行的文本内容

		// 将这一行的内容转换成string类型
		String line = value.toString();

		// 对这一行的文本按特定分隔符切分
		String[] words = StringUtils.split(line, " ");

		// 遍历这个单词数组输出为kv形式 k：单词 v ： 1
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}

	}
}
