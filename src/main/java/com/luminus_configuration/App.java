package com.luminus_configuration;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

import com.luminus.exception.LuminusException;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		Integer k = 3;
		Integer columnas = 13;
		Boolean buscar = false;
		ArrayList<Double> origen = new ArrayList<Double>();
		origen.add(14.2);
		origen.add(13.7);
		ArrayList<Integer> posiciones = new ArrayList<Integer>();
		posiciones.add(14);
		posiciones.add(15);
		String elemento = "\"VENUSTIANO CARRANZA                     \"";
		String salidaCompleta = "/user/luminus/salidacompleta.txt";
		String salidaOrdenadaCompleta = "/user/luminus/salidaordenadacompleta.txt";
		String salidaK = "/user/luminus/salidak.txt";
		String salidaExcel = "/user/luminus/salidafinal.ods";
		LuminusConfiguration knn = new LuminusConfiguration(k, columnas, buscar, elemento, origen, posiciones,
				salidaOrdenadaCompleta, salidaCompleta, salidaK, salidaExcel);
//		knn.createConfiguration();
//		knn.uploadConfigurations();
		try {
//			knn.getDataFromFile("192.168.0.8:9000", "/user/luminus/datos.txt", System.out);
//			knn.uploadFileToHDFS("/home/maestroluminuscom/Escritorio/procedimiento.txt", "/user/luminus/");
			//knn.deleteFromHDFS("/user/luminus/configuracion.properties");
//			knn.getDataFromFile("/user/luminus/procedimiento.txt", System.out);
//			knn.deleteFromHDFS("/user/cachiporra/");
			//knn.makeHDFSDirectory("/user/cachiporra/");
//			System.err.println(knn.existsInHDFS("/user/luminus/proc.txt"));
			// knn.uploadFileToHDFS("/home/maestroluminuscom/Descargas/configuracion.properties",
			// "/user/luminus/configuracion.properties");
			
//			knn.createConfiguration();
//			knn.getDataFromFile("/user/luminus/configuracion.properties", System.out);
		} catch (Exception e) {
			e.printStackTrace();
		}
		knn.runKNN();
	}
}
