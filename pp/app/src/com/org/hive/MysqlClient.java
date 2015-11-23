package com.org.hive;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;





public class MysqlClient {
	
	public static Connection getConnection() {
		Connection conn=null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		try {
			conn =	DriverManager.getConnection("jdbc:mysql://10.91.227.145:3308/test","jack","Jack1@34");
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
		System.out.println(MysqlClient.getConnection());
		
	}
}
