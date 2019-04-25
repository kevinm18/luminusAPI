package com.luminus.knn.mapreduce;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.luminus.hdfs.access.HDFSAccess.Access;
import com.luminus.knn.mapreduce.MapKNN.Map;
import com.luminus.knn.mapreduce.ReduceKNN.Reduce;

public class LuminusKNN extends Configured implements Tool {

	public static boolean esEntero(String cadena) {
		try {
			Integer.parseInt(cadena);
			return true;
		} catch (NumberFormatException excepcion) {
			return false;
		}
	}

	public static void main(String[] args) throws Exception {
//		ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("localFS.txt"));
//		objectOutputStream.writeObject(fileSystem);
//		objectOutputStream.flush();
//		objectOutputStream.close();
		int exit = ToolRunner.run(new LuminusKNN(), args);
		System.exit(exit);
	}

	public int run(String[] arg0) throws Exception {
		Configuration c = new Configuration();
		Path input = new Path(arg0[0]); // donde voy a tomar el archivo
		Path output = new Path(arg0[1]); // donde voy a dejar la salida
		FileSystem fileSystem = Access.getFileSystem(Access.getLocalNetAddress());
		String localNetAddress = Access.getLocalNetAddress();
//		ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("localFS.txt"));
//		FileSystem fileSystem = (FileSystem) objectInputStream.readObject();
		// c.setInt("prueba",5);
		Job j = new Job(c, "wordcount");
		j.setJarByClass(LuminusKNN.class);
		j.setMapperClass(Map.class); // donde esta el map
		j.setReducerClass(Reduce.class); // donde esta el reduce
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output); // de los argumentos que lei , mi entrada y mi salida
		String valork = "";
		String pathsalidacompleta = "";
		String pathsalidaordenadacompleta = "";
		String pathsalidak = "";
		String pathsalidaexcel = "";
		Integer numk;
		HashMap<Double, String> puntosobtenidos = new HashMap<Double, String>();
		ArrayList<String> completo = new ArrayList<String>();
		try {
			Properties prop = new Properties();
			// Path pt = new Path("hdfs:/user/luminus/configuracion.properties");// Location
			// of file in HDFS
//			Path pt = new Path("/home/maestroluminuscom/Descargas/configuracion.properties");
			Access.getDataFromFile("/user/luminus/configuracion.properties",
					new DataOutputStream(new FileOutputStream("configuracion.properties")), fileSystem,
					localNetAddress);
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			BufferedReader br = new BufferedReader(new FileReader("configuracion.properties"));
			prop.load(br);
			valork = prop.getProperty("K");
			pathsalidacompleta = prop.getProperty("pathsalidacompleta");
			pathsalidaordenadacompleta = prop.getProperty("pathsalidaordenadacompleta");
			pathsalidak = prop.getProperty("pathsalidak");
			pathsalidaexcel = prop.getProperty("pathsalidaexcel");
//			Access.createLocalFile("salidaordenadacompleta.txt");
//			Access.createLocalFile("salidacompleta.txt");
//			Access.createLocalFile("salidak.txt");
//			Access.createLocalFile("salidafinal.ods");
//			Access.uploadFileToHDFS("salidacompleta.txt", pathsalidacompleta, fileSystem, true);
//			Access.uploadFileToHDFS("salidaordenadacompleta.txt", pathsalidaordenadacompleta, fileSystem, true);
//			Access.uploadFileToHDFS("salidak.txt", pathsalidak, fileSystem, true);
//			Access.uploadFileToHDFS("salidafinal.ods", pathsalidaexcel, fileSystem, true);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (esEntero(valork)) {
			numk = Integer.valueOf(valork);
		} else {
			numk = Integer.valueOf(valork);
		}
		try {
			// Path pt=new Path("hdfs:/user/luminus/knn2/resumay13/part-r-00000");
			// Path pt=new Path("hdfs:/user/luminus/knn2/salidacompleta.txt");
//			Path pt = new Path(pathsalidacompleta);
//			FileSystem fs = FileSystem.get(new Configuration());
			Access.getDataFromFile(pathsalidacompleta, new DataOutputStream(new FileOutputStream("salidacompleta.txt")),
					fileSystem, localNetAddress);
			BufferedReader br = new BufferedReader(new FileReader("salidacompleta.txt"));
			String linealeida;
			linealeida = br.readLine();
			while (linealeida != null) {
				// System.out.println(line);
				String[] resultado = linealeida.split(",");
				puntosobtenidos.put(Double.parseDouble(resultado[0]), resultado[1]);
				completo.add(linealeida);
				linealeida = br.readLine();// esta leyendo la siguiente
			}
		} catch (Exception e) {
		}
		// double ordenar[] =new double[puntosobtenidos.size()];
		List<Double> ordenar = new ArrayList<Double>();
		// int x=0;
		for (Double n : puntosobtenidos.keySet()) {
			// ordenar[x]=n;
			ordenar.add(n);
			// x++;
		}
		// Arrays.sort(ordenar);
		Collections.sort(ordenar);
		// este es el codigo que escribe en un archivo

		if (Access.existsInHDFS(pathsalidaordenadacompleta, fileSystem)) {
			Access.deleteFromHDFS(pathsalidaordenadacompleta, fileSystem);
		}

		// Escritura del archivo salidaordenadacompleta.txt.
		DataOutputStream stm1 = new DataOutputStream(new FileOutputStream("salidaordenadacompleta.txt"));
		// stm.writeBytes("holi");
		for (int i = 0; i < ordenar.size(); i++) {
			stm1.writeBytes(
					"Distancia: " + ordenar.get(i) + " que corresponde a: " + puntosobtenidos.get(ordenar.get(i)));
			stm1.writeBytes("\n");
		}
		stm1.close();
		Access.uploadFileToHDFS("salidaordenadacompleta.txt", pathsalidaordenadacompleta, fileSystem, true);

		if (Access.existsInHDFS(pathsalidak, fileSystem)) {
			Access.deleteFromHDFS(pathsalidak, fileSystem);
		}
//		DataOutputStream stm = fs.create(escribir);
		DataOutputStream stm = new DataOutputStream(new FileOutputStream("salidak.txt"));
		// stm.writeBytes("holi");
		ArrayList<String> elemento = new ArrayList<String>();
		ArrayList<Integer> repite = new ArrayList<Integer>();
		stm.writeBytes("se solicito un k=" + numk + " se listan estos k nodos \n");
		for (int i = 0; i < ordenar.size(); i++) {
			if (i < numk) {
				stm.writeBytes(
						"Distancia: " + ordenar.get(i) + " que corresponde a: " + puntosobtenidos.get(ordenar.get(i)));
				stm.writeBytes("\n");
				String despuesdeclase = puntosobtenidos.get(ordenar.get(i)).substring(
						puntosobtenidos.get(ordenar.get(i)).indexOf("clase"),
						puntosobtenidos.get(ordenar.get(i)).indexOf("aparece:"));
				if (elemento.contains(despuesdeclase)) {
					int value = elemento.indexOf(despuesdeclase);
					int contenido = repite.get(value);
					repite.set(value, contenido + 1);
				} else {
					elemento.add(despuesdeclase);
					repite.add(1);
				}
			}
		}
		int posicion = 0;
		int mayor = -1;
		for (int i = 0; i < elemento.size(); i++) {
			if (repite.get(i) > mayor) {
				posicion = i;
				mayor = repite.get(i);
			}
		}
		// stm.writeBytes("dime algo"+posicion+""+mayor);
		for (int i = 0; i < elemento.size(); i++) {
			if (i == 0) {
				if (mayor == 1) {
					stm.writeBytes("no hay una clase a la que pertenezca ya que los resultados no fueron concluyentes");
				} else {
					stm.writeBytes("pertenece a" + elemento.get(i) + " la cual forma parte de " + mayor + "k cercanos");
				}
			}
		}
		stm.close();
		Access.uploadFileToHDFS("salidak.txt", pathsalidak, fileSystem, true);
		// j.getConfiguration().setInt("prueba",5);
		/*
		 * Generacion del archivo excel
		 */

		if (Access.existsInHDFS(pathsalidaexcel, fileSystem)) {
			Access.deleteFromHDFS(pathsalidaexcel, fileSystem);
		}
		// Flujo de datos de salida para poder escribir el archivo Excel.
		DataOutputStream streamExcel = new DataOutputStream(new FileOutputStream("salidaexcel.ods"));
		// Mapa en el que se guardaran los datos leidos
		HashMap<Double, String> mapaDatosExcel = new HashMap<Double, String>();
		try {
			// Lectura de cada linea del archivo salidaordenadacompleta.txt
			BufferedReader lectorExcel = new BufferedReader(new FileReader("salidaordenadacompleta.txt"));
			String filaExcel = lectorExcel.readLine();
			while (filaExcel != null) {
				String[] fila = filaExcel.split(",");
				mapaDatosExcel.put(Double.parseDouble(fila[0]), fila[1]);
				filaExcel = lectorExcel.readLine();
			}
		} catch (Exception e) {

		}
		// Extrae clases
		HashMap<Double, String> puntosObtenidosParaExcel = puntosobtenidos;
//		List<String> listaCoordenadas = new ArrayList<>();
//		List<String> listaClases = new ArrayList<>();
//		List<String> listaVecesQueAparece = new ArrayList<>();
		for (Double puntoSubcadenaClase : puntosObtenidosParaExcel.keySet()) {

		}
		// Escritura de cada punto en el archivo salidaexcel.xsl
		streamExcel.writeBytes("Distancia\t Ubicacion\n");
		List<String> subcadenas = new ArrayList<String>();
		for (Double punto : ordenar) {
			subcadenas.add(puntosObtenidosParaExcel.get(punto));
		}
		// Lectura de las subcadenas para obtener los demas resultados
		for (String sub : subcadenas) {

		}
		Integer contador = 0;
		for (Double punto : ordenar) {
			streamExcel.writeBytes(punto.toString() + "\t" + subcadenas.get(contador) + "\n");
			contador++;
		}
		streamExcel.close();
		Access.uploadFileToHDFS("salidaexcel.ods", pathsalidaexcel, fileSystem, true);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

		return 0;
	}
	
}
