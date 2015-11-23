package com.org.db.utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnection {
	
	public static Connection getHiveConnection(){
		String hivedirver=PropContext.PRO.getProperty("hivedriver");
		String hiveurl=PropContext.PRO.getProperty("hiveurl");
	//	String hivedb=PropContext.PRO.getProperty("hivedb");
		Connection conn=null;
		try {
			Class.forName(hivedirver);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		try {
			conn=DriverManager.getConnection(hiveurl, "", "");
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		return conn;
		
		
	}
	public static void close(Closeable... conn){
		for (Closeable c:conn){
			if (null!=c){
				try {
					c.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
public static void main(String[] args) {
	System.out.println(HiveConnection.getHiveConnection());
	System.out.println(MysqlConnection.getMysqlConnection());
}
}
