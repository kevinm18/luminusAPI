/**
 * 
 */
package com.luminus.hdfs.access;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

import com.luminus.exception.LuminusException;

/**
 * @author root
 *
 */
public class HDFSAccess {
	public static class Access {

		/**
		 * 
		 * @param path
		 * @param outputStream
		 * @param fileSystem
		 * @param localNetAddress
		 * @throws IOException
		 * @throws LuminusException
		 */
		public static void getDataFromFile(String path, OutputStream outputStream, FileSystem fileSystem,
				String localNetAddress) throws IOException, LuminusException {
			if (existsInHDFS(path, fileSystem)) {
				String uri = "hdfs://" + localNetAddress + ":9000" + path;
				InputStream inStream = null;
				try {
					inStream = fileSystem.open(new Path(uri));
					IOUtils.copyBytes(inStream, outputStream, 4096, false);
				} finally {
					IOUtils.closeStream(inStream);
				}
			} else {
				throw new LuminusException(1);
			}
		}

		/**
		 * 
		 * @param path
		 * @param fileSystem
		 * @param localNetAddress
		 * @throws Exception
		 * @throws LuminusException
		 */
		public static void makeHDFSDirectory(String path, FileSystem fileSystem, String localNetAddress)
				throws Exception, LuminusException {
			String uri = "hdfs://" + localNetAddress + ":9000" + path;
			if (!existsInHDFS(path, fileSystem)) {
				fileSystem.mkdirs(new Path(uri));
			} else {
				throw new LuminusException(3);
			}

		}

		/**
		 * 
		 * @param localPath
		 * @param hdfsPath
		 * @param fileSystem
		 * @param overwrite
		 * @throws IOException
		 * @throws LuminusException
		 */
		public static void uploadFileToHDFS(String localPath, String hdfsPath, FileSystem fileSystem, Boolean overwrite)
				throws IOException, LuminusException {
//			if (existsInHDFS(hdfsPath, fileSystem) || !overwrite) {
//				throw new LuminusException(2);
//			} else {
				fileSystem.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
//			}
		}

		/**
		 * 
		 * @param path
		 * @param fileSystem
		 * @throws IllegalArgumentException
		 * @throws IOException
		 * @throws LuminusException
		 */
		@SuppressWarnings("deprecation")
		public static void deleteFromHDFS(String path, FileSystem fileSystem)
				throws IllegalArgumentException, IOException, LuminusException {
			if (existsInHDFS(path, fileSystem)) {
				fileSystem.delete(new Path(path));
			} else {
				throw new LuminusException(1);
			}
		}

		/**
		 * 
		 * @param path
		 * @param fileSystem
		 * @return
		 * @throws IllegalArgumentException
		 * @throws IOException
		 */
		public static Boolean existsInHDFS(String path, FileSystem fileSystem)
				throws IllegalArgumentException, IOException {
			return fileSystem.exists(new Path(path));
		}

		/**
		 * 
		 * @param localNetAddress
		 * @return
		 */
		public static FileSystem getFileSystem(String localNetAddress) {
			String uri = "hdfs://" + localNetAddress + ":9000";
			try {
				return FileSystem.get(URI.create(uri), new Configuration());
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		/**
		 * 
		 * @return
		 */
		public static String getLocalNetAddress() {
			try {
				return InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				e.printStackTrace();
				return null;
			}
		}
		
		/**
		 * 
		 * @param fileName
		 */
		public static void createLocalFile(String fileName) {
			try {
				FileOutputStream fileOutputStream = new FileOutputStream(fileName);
				fileOutputStream.flush();
				fileOutputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
