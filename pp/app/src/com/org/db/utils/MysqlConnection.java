package com.org.db.utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlConnection {
	
	
	public static Connection getMysqlConnection() {
		String mysqldriver=PropContext.PRO.getProperty("mysqldriver");
		String mysqlurl=PropContext.PRO.getProperty("mysqlurl");
		String mysqlpassword=PropContext.PRO.getProperty("mysqlpassword");
		String mysqluser=PropContext.PRO.getProperty("mysqluser");
//		System.out.println(mysqldriver+"----"+mysqlurl+"----"+mysqluser+"-----"+mysqlpassword);
		Connection conn=null;
		
		try {
			Class.forName(mysqldriver);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		try {
			conn =	DriverManager.getConnection(mysqlurl,mysqluser,mysqlpassword);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		return conn;
	}
	
	public static void close(Closeable... close){
		for (Closeable c:close){
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
	System.out.println(MysqlConnection.getMysqlConnection());
	
}
}
