package com.org.hive;
import java.io.File;
import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import com.org.db.utils.HiveConnection;
import com.org.db.utils.MysqlConnection;
public class DownloadDataFromHive {

	Connection hiveConn = null;
	Connection mysqlConn = null;
	String hivesql;
	String mysql;
	String path;

	public DownloadDataFromHive() {
		super();
		this.hiveConn = HiveConnection.getHiveConnection();

		this.mysqlConn = MysqlConnection.getMysqlConnection();
	}

	public void close() {

		if (null != hiveConn) {
			try {
				hiveConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (null != mysqlConn) {
			try {
				mysqlConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void setHivesql(String hivesql) {
		this.hivesql = hivesql;
		//System.out.println(this.hivesql);
	}

	public void setMysql(String mysql) {
		this.mysql = mysql;
		//System.out.println(this.mysql);
	}

	public void setPath(String path) {
		Date dt = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		this.path = path + "/" + sdf.format(dt);
	}

	public String getPath() {
		return this.path;
	}

	public String getFilePath() {
		File f= new File(this.path);
		if (!f.exists() || !f.isDirectory()) {
			return null;
		}
		File[] files = f.listFiles();
		System.out.println(Arrays.toString(files));
		File temp = files[0];
		for (File file : files) {
			if (file.lastModified() >= temp.lastModified()&& ! file.getName().contains(".crc")) {
				temp = file;
				System.out.println(file);
			}
		}
		if (temp.getName().contains(".crc")){
			return null;
		}
		return temp.getAbsolutePath();
	}

	public boolean executeHiveSql() throws SQLException {
		if (this.hivesql != "" && null != this.hivesql) {
			System.out.println(this.hivesql + hiveConn);
			// this.hiveConn.setCatalog(PropContext.PRO.getProperty("hivedb"));
			Statement st = hiveConn.createStatement();
			// st.execute("use "+PropContext.PRO.getProperty("hivedb"));
			return st.execute(this.hivesql);
		}

		return false;
	}

	public boolean executeMysql() throws SQLException {
		if (this.mysql != "" && null != this.mysql) {
			mysqlConn.setClientInfo("local-infile", "1");
			Statement st = mysqlConn.createStatement();
			return st.execute(this.mysql);

		}
		return false;
	}

	public static void main(String[] args) throws SQLException {
		DownloadDataFromHive ddfh = new DownloadDataFromHive();
		ddfh.setPath("/hdfs/outfile");
		ddfh.setHivesql("use hdfs");
		ddfh.executeHiveSql();
		ddfh.setHivesql("drop table  if EXISTS tmp_reportdata101");
		ddfh.executeHiveSql();
//		ddfh.getPath();
		ddfh.setHivesql("create table tmp_reportdata101 as select deviceid ,max(speed) maxspeed ,avg(speed) as avgspeed,min(speed) as minspeed from t_reportData101 group by deviceid");
		ddfh.executeHiveSql();
		ddfh.setHivesql("insert overwrite local directory '"+ddfh.path+"' row format delimited fields terminated by '\\t'select * from tmp_reportdata101");
		ddfh.executeHiveSql();
		System.out.println(ddfh.getFilePath());
		// select max(speed) maxspeed ,avg(speed) as avgspeed,min(speed) as
		// minspeed from t_reportData101;
		// ddfh.setHivesql("select count(*) from t_test_1");
		// ddfh.executeHiveSql();
		// ddfh.close();
		ddfh.setMysql("load data local infile '"+ddfh.getFilePath()+"' replace into table t_reportdata101 fields terminated by '\t'");
//		ddfh.setMysql("load data local infile '"+"F:/i.txt"+"' replace into table t_reportdata101 fields terminated by '\t'");
		ddfh.executeMysql();
		ddfh.close();
		System.out.println("compete");
		// System.out.println(ddfh.getFilePath(new File("F:/hive")));
	}
}
