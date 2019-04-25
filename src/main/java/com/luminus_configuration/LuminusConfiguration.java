package com.luminus_configuration;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.RunJar;
import org.apache.zookeeper.common.IOUtils;

import com.luminus.exception.LuminusException;
import com.luminus.hdfs.access.HDFSAccess.Access;
import com.luminus.knn.mapreduce.LuminusKNN;
import com.luminus.knn.mapreduce.MapKNN.Map;
import com.luminus.knn.mapreduce.ReduceKNN.Reduce;
import com.luminus.prueba.mapreduce.Driver;

public class LuminusConfiguration {

	/*
	 * Atributos de la clase.
	 */

	/**
	 * {@link Integer} k Numero de vecinos que se van a buscar al ejecutar el
	 * algoritmo KNN.
	 */
	private Integer k;
	/**
	 * {@link Integer} columnPositions Posiciones de las columnas.
	 */
	private Integer columnPositions;
	/**
	 * {@link Boolean} search ??
	 */
	private Boolean search;
	/**
	 * {@link String} searchElement Elemento con el que se van a comparar todos los
	 * vecinos.
	 */
	private String searchElement;
	/**
	 * 
	 */
	private ArrayList<Double> origin;
	/**
	 * 
	 */
	private ArrayList<Integer> positions;
	/**
	 * 
	 */
	private String completeOrderedOutputPath;
	/**
	 * 
	 */
	private String completeOutputPath;
	/**
	 * 
	 */
	private String kOutputPath;
	/**
	 * 
	 */
	private String spreadsheetOutputPath;

	/*
	 * Metodos de configuracion.
	 */

	/**
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	private void createFile(String fileName) {
		Access.createLocalFile(fileName);
	}

	/**
	 * 
	 * @throws IOException
	 */
	public void createConfiguration() {
		// Crea el archivo properties.
		String fileName = "configuracion.properties";
		createFile(fileName);
		try {
			@SuppressWarnings("resource")
			DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(fileName));
			dataOutputStream.writeBytes("posicionescolumnas=" + this.getColumnPositions().toString() + "\n");
			dataOutputStream.writeBytes("buscar=" + (this.getSearch() ? "S" : "N") + "\n");
			dataOutputStream.writeBytes("elementoabuscar=" + this.getSearchElement() + "\n");
			dataOutputStream.writeBytes("K=" + this.getK().toString() + "\n");
			dataOutputStream.writeBytes("pathsalidaordenadacompleta=" + this.getCompleteOrderedOutputPath() + "\n");
			dataOutputStream.writeBytes("pathsalidacompleta=" + this.getCompleteOutputPath() + "\n");
			dataOutputStream.writeBytes("pathsalidak=" + this.getkOutputPath() + "\n");
			dataOutputStream.writeBytes("pathsalidaexcel=" + this.getSpreadsheetOutputPath() + "\n");
			String originProp = "";
			Integer counter = 1;
			for (Double d : this.getOrigin()) {
				originProp = originProp + d.toString();
				if (counter == this.getOrigin().size()) {
					break;
				}
				originProp = originProp + ",";
				counter++;
			}
			dataOutputStream.writeBytes("origen=" + originProp + "\n");
			String positionProp = "";
			counter = 1;
			for (Integer d : this.getPositions()) {
				positionProp = positionProp + d.toString();
				if (counter == this.getPositions().size()) {
					break;
				}
				positionProp = positionProp + ",";
				counter++;
			}
			dataOutputStream.writeBytes("posiciones=" + positionProp + "\n");
			try {
				if (!Access.existsInHDFS("/user/luminus/", Access.getFileSystem(Access.getLocalNetAddress()))) {
					makeHDFSDirectory("/user/luminus/");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (IOException e) {

		}
	}

	/**
	 * 
	 * @param host
	 * @param pathToFile
	 * @param outputStream
	 * @throws IOException
	 */
	public void getDataFromFile(String path, OutputStream outputStream) throws IOException {
		try {
			Access.getDataFromFile(path, outputStream, Access.getFileSystem(Access.getLocalNetAddress()),
					Access.getLocalNetAddress());
		} catch (LuminusException e) {
			e.printStackTrace();
			System.err.println(e.getEx());
		}
	}

	/**
	 * 
	 * @param path
	 * @throws Exception
	 */
	public void makeHDFSDirectory(String path) throws Exception {
		try {
			Access.makeHDFSDirectory(path, Access.getFileSystem(Access.getLocalNetAddress()),
					Access.getLocalNetAddress());
		} catch (LuminusException e) {
			e.printStackTrace();
			System.err.println(e.getEx());
		}
	}

	/**
	 * 
	 * @param localPath
	 * @param hdfsPath
	 * @throws IOException
	 */
	public void uploadFileToHDFS(String localPath, String hdfsPath, Boolean overwrite) throws IOException {
		try {
			Access.uploadFileToHDFS(localPath, hdfsPath, Access.getFileSystem(Access.getLocalNetAddress()), overwrite);
		} catch (LuminusException e) {
			e.printStackTrace();
			System.err.println(e.getEx());
		}
	}

	/**
	 * 
	 * @param path
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void deleteFromHDFS(String path) throws IllegalArgumentException, IOException {
		try {
			Access.deleteFromHDFS(path, Access.getFileSystem(Access.getLocalNetAddress()));
		} catch (LuminusException e) {
			e.printStackTrace();
			System.err.println(e.getEx());
		}
	}

	/**
	 * 
	 * @param command
	 */
	private void executeCommand(String[] command) {
		String s = null;
		try {
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			}
			while ((s = stdError.readLine()) != null) {
				System.out.println(s);
			}
			System.exit(0);
		} catch (IOException e) {

		}
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * 
	 */
	public void runKNN() {
		try {
//			DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream("args.txt"));
//			dataOutputStream.writeBytes("/user/luminus/datos.txt");
//			dataOutputStream.writeBytes("/user/luminus/output903");
//			dataOutputStream.flush();
//			dataOutputStream.close();
			String[] args = new String[] { "hdfs://192.168.0.8:9000/user/luminus/datos.txt",
					"hdfs://192.168.0.8:9000/user/luminus/output922" };
			LuminusKNN.main(args);
//			Driver.main(new String[] { "hdfs://192.168.0.8:9000/user/luminus/datos.txt",
//					"hdfs://192.168.0.8:9000/user/luminus/outputPrueba1" });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void uploadConfigurations() {
		try {
			uploadFileToHDFS("configuracion.properties", "/user/luminus/configuracion.properties", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 */
	private Boolean isReadyToExecute(String fileName) {
		Properties propertiesFile = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(fileName);
			propertiesFile.load(inputStream);
			if (propertiesFile.getProperty("posicionescolumnas") == "" || propertiesFile.getProperty("buscar") == ""
					|| propertiesFile.getProperty("elementoabuscar") == "" || propertiesFile.getProperty("origen") == ""
					|| propertiesFile.getProperty("posiciones") == "" || propertiesFile.getProperty("K") == ""
					|| propertiesFile.getProperty("pathsalidaordenadacompleta") == ""
					|| propertiesFile.getProperty("pathsalidacompleta") == ""
					|| propertiesFile.getProperty("pathsalidak") == ""
					|| propertiesFile.getProperty("pathsalidaexcel") == ""
					|| propertiesFile.getProperty("posicionescolumnas") == null
					|| propertiesFile.getProperty("buscar") == null
					|| propertiesFile.getProperty("elementoabuscar") == null
					|| propertiesFile.getProperty("origen") == null || propertiesFile.getProperty("posiciones") == null
					|| propertiesFile.getProperty("K") == null
					|| propertiesFile.getProperty("pathsalidaordenadacompleta") == null
					|| propertiesFile.getProperty("pathsalidacompleta") == null
					|| propertiesFile.getProperty("pathsalidak") == null
					|| propertiesFile.getProperty("pathsalidaexcel") == null) {
				return false;
			}
			return true;
		} catch (IOException e) {
		}
		return false;
	}

	/*
	 * Constructor(es).
	 */

	/**
	 * 
	 * @param k
	 * @param columnPositions
	 * @param search
	 * @param searchElement
	 * @param origin
	 * @param positions
	 * @param completeOrderedOutputPath
	 * @param completeOutputPath
	 * @param kOutputPath
	 * @param spreadsheetOutputPath
	 */
	public LuminusConfiguration(Integer k, Integer columnPositions, Boolean search, String searchElement,
			ArrayList<Double> origin, ArrayList<Integer> positions, String completeOrderedOutputPath,
			String completeOutputPath, String kOutputPath, String spreadsheetOutputPath) {
		super();
		this.k = k;
		this.columnPositions = columnPositions;
		this.search = search;
		this.searchElement = searchElement;
		this.origin = origin;
		this.positions = positions;
		this.completeOrderedOutputPath = completeOrderedOutputPath;
		this.completeOutputPath = completeOutputPath;
		this.kOutputPath = kOutputPath;
		this.spreadsheetOutputPath = spreadsheetOutputPath;
	}

	/*
	 * Getters y Setters.
	 */

	/**
	 * @return the k
	 */
	public Integer getK() {
		return k;
	}

	/**
	 * @param k the k to set
	 */
	public void setK(Integer k) {
		this.k = k;
	}

	/**
	 * @return the columnPositions
	 */
	public Integer getColumnPositions() {
		return columnPositions;
	}

	/**
	 * @param columnPositions the columnPositions to set
	 */
	public void setColumnPositions(Integer columnPositions) {
		this.columnPositions = columnPositions;
	}

	/**
	 * @return the search
	 */
	public Boolean getSearch() {
		return search;
	}

	/**
	 * @param search the search to set
	 */
	public void setSearch(Boolean search) {
		this.search = search;
	}

	/**
	 * @return the searchElement
	 */
	public String getSearchElement() {
		return searchElement;
	}

	/**
	 * @param searchElement the searchElement to set
	 */
	public void setSearchElement(String searchElement) {
		this.searchElement = searchElement;
	}

	/**
	 * @return the origin
	 */
	public ArrayList<Double> getOrigin() {
		return origin;
	}

	/**
	 * @param origin the origin to set
	 */
	public void setOrigin(ArrayList<Double> origin) {
		this.origin = origin;
	}

	/**
	 * @return the positions
	 */
	public ArrayList<Integer> getPositions() {
		return positions;
	}

	/**
	 * @param positions the positions to set
	 */
	public void setPositions(ArrayList<Integer> positions) {
		this.positions = positions;
	}

	/**
	 * @return the completeOrderedOutputPath
	 */
	public String getCompleteOrderedOutputPath() {
		return completeOrderedOutputPath;
	}

	/**
	 * @param completeOrderedOutputPath the completeOrderedOutputPath to set
	 */
	public void setCompleteOrderedOutputPath(String completeOrderedOutputPath) {
		this.completeOrderedOutputPath = completeOrderedOutputPath;
	}

	/**
	 * @return the completeOutputPath
	 */
	public String getCompleteOutputPath() {
		return completeOutputPath;
	}

	/**
	 * @param completeOutputPath the completeOutputPath to set
	 */
	public void setCompleteOutputPath(String completeOutputPath) {
		this.completeOutputPath = completeOutputPath;
	}

	/**
	 * @return the kOutputPath
	 */
	public String getkOutputPath() {
		return kOutputPath;
	}

	/**
	 * @param kOutputPath the kOutputPath to set
	 */
	public void setkOutputPath(String kOutputPath) {
		this.kOutputPath = kOutputPath;
	}

	/**
	 * @return the spreadsheetOutputPath
	 */
	public String getSpreadsheetOutputPath() {
		return spreadsheetOutputPath;
	}

	/**
	 * @param spreadsheetOutputPath the spreadsheetOutputPath to set
	 */
	public void setSpreadsheetOutputPath(String spreadsheetOutputPath) {
		this.spreadsheetOutputPath = spreadsheetOutputPath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ConfigurationKNN [k=" + k + ", columnPositions=" + columnPositions + ", search=" + search
				+ ", searchElement=" + searchElement + ", origin=" + origin + ", positions=" + positions
				+ ", completeOrderedOutputPath=" + completeOrderedOutputPath + ", completeOutputPath="
				+ completeOutputPath + ", kOutputPath=" + kOutputPath + ", spreadsheetOutputPath="
				+ spreadsheetOutputPath + "]";
	}
}
