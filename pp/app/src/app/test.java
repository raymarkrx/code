package app;



import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
public class test {

	 public static void main (String args[]) throws IOException  {	
		 InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
		 Properties p=new Properties();
		 p.load(is);
		 PropertyConfigurator.configure(p);
		 Logger log= Logger.getLogger("ServerDailyRollingFile");
		 log.info("test.......");
		 log.error("error...");
	 }
}
