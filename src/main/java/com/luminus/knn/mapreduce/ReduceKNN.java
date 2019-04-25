package com.luminus.knn.mapreduce;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.luminus.exception.LuminusException;
import com.luminus.hdfs.access.HDFSAccess.Access;

public class ReduceKNN {
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		DataOutputStream stm = null;
		DataOutputStream streamExcel = null;
		String pathsalidacompleta = null;

		protected void setup(Context context) throws IOException, InterruptedException {

//			FileSystem fs = FileSystem.get(new Configuration());
//			Path escribir = new Path("hdfs:/user/luminus/salidacompleta.txt");
//			if (fs.exists(escribir)) {
//				fs.delete(escribir, true);
//			}
//			stm = fs.create(escribir);
			Properties prop = new Properties();
			BufferedReader br = new BufferedReader(new FileReader("configuracion.properties"));
			prop.load(br);
			pathsalidacompleta = prop.getProperty("pathsalidacompleta");
			FileSystem fileSystem = Access.getFileSystem(Access.getLocalNetAddress());
			if (Access.existsInHDFS(pathsalidacompleta, fileSystem)) {
				try {
					Access.deleteFromHDFS("/user/luminus/salidacompleta.txt", fileSystem);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (LuminusException e) {
					e.printStackTrace();
				}
			}
			stm = new DataOutputStream(new FileOutputStream("salidacompleta.txt"));
			try {
				Access.uploadFileToHDFS("salidacompleta.txt", pathsalidacompleta, fileSystem, true);
			} catch (LuminusException e1) {
				e1.printStackTrace();
			}
//			try {
//				Access.getDataFromFile("/user/luminus/salidacompleta.txt", new FileOutputStream("salidacompleta.txt"),
//						Access.getFileSystem(Access.getLocalNetAddress()), Access.getLocalNetAddress());
//			} catch (LuminusException e) {
//				e.printStackTrace();
//			}
			
		}

		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			String line = word.toString();
			String[] resultado = line.split(",");
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			stm.writeBytes(line + " aparece: " + sum);
			stm.writeBytes("\n");
			con.write(word, new IntWritable(sum));
			Properties prop = new Properties();
			BufferedReader br = new BufferedReader(new FileReader("configuracion.properties"));
			prop.load(br);
			pathsalidacompleta = prop.getProperty("pathsalidacompleta");
			try {
				Access.uploadFileToHDFS("salidacompleta.txt", pathsalidacompleta,
						Access.getFileSystem(Access.getLocalNetAddress()), true);
			} catch (LuminusException e) {
				e.printStackTrace();
			}
		}
	}
}
