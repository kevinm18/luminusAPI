package com.luminus.prueba.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
	public static void main(String [] args) throws Exception {
		int exit = ToolRunner.run(new Driver(), args);
		System.exit(exit);
	}

	public int run(String[] arg0) throws Exception {
		Configuration c = new Configuration();
		Path input = new Path(arg0[0]); // donde voy a tomar el archivo
		Path output = new Path(arg0[1]); // donde voy a dejar la salida

		// c.setInt("prueba",5);
		Job j = new Job(c, "DriverPrueba");
		j.setMapperClass(MapPrueba.class); // donde esta el map
		j.setReducerClass(IntSumReducer.class); // donde esta el reduce
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		return j.waitForCompletion(true) ? 0 : 1;
		}
}
