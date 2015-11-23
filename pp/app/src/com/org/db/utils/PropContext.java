package com.org.db.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropContext {
	public static final Properties PRO=getProp();
		private static Properties getProp(){
			
			
			InputStream is =PropContext.class.getResourceAsStream("db.properties");
			
			Properties pro=new Properties();
			try {
				pro.load(is);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			System.out.println("get connection");
			System.out.println(pro.getProperty("hivedriver"));
			return pro;
			
		}
}
