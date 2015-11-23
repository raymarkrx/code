package com.org.hive;

import java.sql.SQLException;

public class CarStartStatus {
public static void main(String[] args) throws SQLException {
	DownloadDataFromHive ddfh = new DownloadDataFromHive();
	ddfh.setPath("/home/hdfs/outfile");
	ddfh.setHivesql("use aichuche_db");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_1");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_2");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_3");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_4");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_5");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_6");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS ts_7");
	ddfh.executeHiveSql();
	//step1
		ddfh.setHivesql("create table ts_1 as select deviceid,dt,speed,lag(speed,1,0) over (partition by deviceid order by dt) as lag_speed1,"
				+ "lag(speed,2,0) over (partition by deviceid order by dt) as lag_speed2,lead(speed,1,0) over (partition by deviceid order by dt) as lead_speed1"
				+ ",lead(speed,2,0) over (partition by deviceid order by dt) as lead_speed2 from t_data101");
		ddfh.executeHiveSql();
		
	//step2
	ddfh.setHivesql("create table ts_2 as select deviceid,speed,dt,case when (speed=0 and lag_speed1=0 and lag_speed2=0 and "
			+ "lead_speed1>0 and lead_speed2>0 and lead_speed1<lead_speed2 ) then 's'when (speed-lead_speed1>0) then 'e' end status from ts_1");
	ddfh.executeHiveSql();
	
	//step3
	ddfh.setHivesql("create table ts_3 as select deviceid,dt,lead_datetime from("
			+ "select deviceid ,dt,lead(dt,1,null) over (partition by deviceid order by dt) as lead_datetime,status "
			+ " from ts_2 where status ='s' or status ='e') a where status ='s'");
	ddfh.executeHiveSql();
	//step4
	ddfh.setHivesql("create table ts_4 as select a.*,b.dt as start_datetime ,b.lead_datetime from t_data101 a join ts_3 b where a.dt between b.dt and  b.lead_datetime "
			+ " and a.deviceid=b.deviceid");
	ddfh.executeHiveSql();	

	//step5
	ddfh.setHivesql("create table ts_5 as select deviceid,dt,lag(dt,1,0) over "
				+ " (partition by deviceid,start_datetime,lead_datetime order by dt) as lag_datatime,speed,lag(speed,1,null) over "
				+ " (partition by deviceid,start_datetime,lead_datetime order by dt) as lag_speed from ts_4");
	ddfh.executeHiveSql();	

	//step6
	ddfh.setHivesql("create table ts_6 as select deviceid,(speed-lag_speed)/(unix_timestamp(dt)-unix_timestamp(lag_datatime)) "
			+ " as ax from ts_5 where lag_datatime is not null");
	ddfh.executeHiveSql();	

	//step7
	ddfh.setHivesql("create table ts_7 as select deviceid,percentile(cast(ax*1000 as bigint),0.75)/1000 as ax from ts_6 group by deviceid ");
	ddfh.executeHiveSql();	
	
//	ddfh.setHivesql("insert overwrite local directory '"+ddfh.path+"' row format delimited fields terminated by '\\t'select * from ts_7");
//	ddfh.executeHiveSql();
//	System.out.println(ddfh.getFilePath());
//	ddfh.setMysql("load data local infile '"+ddfh.getFilePath()+"' replace into table t_carstartstatus fields terminated by '\t'");
//	ddfh.executeMysql();
	System.out.println("complete");
}
}
