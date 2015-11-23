package com.org.db.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MyLogger {
	static{
		
		 InputStream is=MyLogger.class.getResourceAsStream("log4j.properties");
		 Properties p=new Properties();
		 try {
			p.load(is);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 PropertyConfigurator.configure(p);
//		 Logger log= Logger.getLogger("ServerDailyRollingFile");
	}
	
	public static Logger getlogger(String s){
		return Logger.getLogger(s);
	}
	public static void main(String[] args) {
		MyLogger.getlogger("ServerDailyRollingFile").info("use123 ....");
		MyLogger.getlogger("server").info("use12 ....");
	}
}
