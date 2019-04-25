package com.luminus.knn.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapKNN {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private double[] origenanalisis;
		private int[] posiciones;
		int[] posicioncolum; // posiciones en el archivo de las clases
		String[] elementos;
		String buscar = "";
		// private int conteo = 0;
		boolean encabezados = false;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String origen = "";
			String posicio = "";
			String posicionescolumnas = "";
			String posicolum = "";

			String elementoabuscar = "";
			// conteo++;
			try {
				Properties prop = new Properties();
				//Path pt = new Path("hdfs:/user/luminus/configuracion.properties");// Location of file in HDFS
				// FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new FileReader("configuracion.properties"));
				prop.load(br);
				origen = prop.getProperty("origen");
				posicio = prop.getProperty("posiciones");
				posicolum = prop.getProperty("posicionescolumnas");
				buscar = prop.getProperty("buscar");
				if (buscar.equals("S")) {
					elementoabuscar = prop.getProperty("elementoabuscar");
					elementos = elementoabuscar.split(",");
				}

			} catch (Exception e) {
			}
			String[] origenarray = origen.split(","); // para obtener todo lo que compone origen
			// String array[] = new String[10];
			origenanalisis = new double[origenarray.length]; // para guardarlo como double
			String[] posicioarray = posicio.split(","); // guardar todas las posiciones a comparar
			posiciones = new int[posicioarray.length];
			for (int i = 0; i < origenarray.length; i++) {
				origenanalisis[i] = Double.parseDouble(origenarray[i]);
			}
			for (int i = 0; i < posicioarray.length; i++) {
				posiciones[i] = Integer.parseInt(posicioarray[i]) - 1;
			}
			String[] posicolum1 = posicolum.split(",");
			posicioncolum = new int[posicolum1.length];
			for (int i = 0; i < posicolum1.length; i++) {
				posicioncolum[i] = Integer.parseInt(posicolum1[i]) - 1;
			}
		}

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			if (encabezados == true) {
				Configuration conf = con.getConfiguration();
				// int intPrueba = conf.getInt("prueba", 0);

				// VAMOS CON EL CASO GENERAL, SOLO CALCULAR LA DISTANCIA CON TODOS LOS PUNTOS
				// POSICIONES 14, 15 y origen 14.2 y 13.7
				boolean error = false;
				String line = value.toString(); // toma una linea del texto
				String[] words = line.split(","); // busca dentro de esa linea , palabras separadas por una comita
				double coordenadasevaluar[] = new double[posiciones.length];
				for (int i = 0; i < posiciones.length; i++) { // para todas las posiciones a evaluar 13,14
					if (words[posiciones[i]] != null) {
						boolean result = esDoble(words[posiciones[i]]);
						if (result == true) {
							coordenadasevaluar[i] = Double.parseDouble(words[posiciones[i]]);
						} else {
							boolean resint = esEntero(words[posiciones[i]]);
							if (resint == true) {
								coordenadasevaluar[i] = Integer.parseInt(words[posiciones[i]]);
							} else { // no lo voy a poder evaluar
								error = true;
							}
						}
					}
				}
				if (error == false) { // solo para validar que si sea entero o flotante
					double distancia = calculardistancia(origenanalisis, coordenadasevaluar);
					// calcular la distancia que existe entre el elemento que me dieron y el que
					// estoy obteniendo del archivo
					// todas las coordenas a las que se le calcula distancia
					String completa = "";
					for (int i = 0; i < coordenadasevaluar.length; i++) {
						// completa=completa+" "+String.valueOf(coordenadasevaluar[i]+"
						// "+words[posiciones[i]]);
						completa = completa + "  " + String.valueOf(coordenadasevaluar[i]);
					}
					// las clases
					String clases = "";
					// si soporta multiples clases
					int contador = 0;
					for (int i = 0; i < posicioncolum.length; i++) {
						if (buscar.equals("S"))
							if (words[posicioncolum[i]].equals(elementos[i])) {
								contador++;
							}
						clases = clases + "  " + words[posicioncolum[i]];

					}
					if (contador == posicioncolum.length || buscar.equals("N")) {
						Text outputKey = new Text(String.valueOf(distancia) + ", " + completa + " clase:" + clases);
						IntWritable outputValue = new IntWritable(1);
						con.write(outputKey, outputValue);
					}
				}
			} else {
				encabezados = true;
			}
		}

		public static boolean esEntero(String cadena) {
			try {
				Integer.parseInt(cadena);
				return true;
			} catch (NumberFormatException excepcion) {
				return false;
			}
		}

		public boolean esDoble(String palabra) {
			try {
				Double.parseDouble(palabra);
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}

		public double calculardistancia(double[] c1, double[] c2) {
			double result = 0.0;
			double todo = 0.0;
			for (int i = 0; i < c1.length; i++) {
				result = c1[i] - c2[i];
				result = Math.pow(result, 2);
				todo = todo + result;
			}
			double raiz = Math.sqrt(todo);
			return raiz;
		}
	}
}
