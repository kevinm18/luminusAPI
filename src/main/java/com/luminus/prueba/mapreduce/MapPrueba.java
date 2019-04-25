package com.luminus.prueba.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapPrueba extends Mapper<LongWritable, Text, Text, IntWritable>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(new Text(value.toString()), new IntWritable(1));
	}
}
