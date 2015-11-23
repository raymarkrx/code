package com.org.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class HiveClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive://10.91.228.28:10008/hdfs", "", "");
		Statement stmt = con.createStatement();
		String tableName = "t_test_1";
		//
		// stmt.executeQuery("drop table " + tableName);
		//
		// ResultSet res = stmt.executeQuery("create table " + tableName +
		// " (key int, value string)");

		// show tables
		stmt.executeQuery("use hdfs");
		String sql = "show tables ";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "desc " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table

		// NOTE: filepath has to be local to the hive server

		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// regular hive query
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

}
