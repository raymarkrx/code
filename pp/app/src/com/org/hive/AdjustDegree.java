package com.org.hive;

import java.sql.SQLException;

public class AdjustDegree {
public static void main(String[] args) throws SQLException {
	DownloadDataFromHive ddfh = new DownloadDataFromHive();
	ddfh.setPath("/home/hdfs/outfile");
	ddfh.setHivesql("use aichuche_db");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_1");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_2");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_3");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_4");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_5");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_6");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_7");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_8");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_9");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_10");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_11");
	ddfh.executeHiveSql();
	ddfh.setHivesql("drop table  if EXISTS table_12");
	ddfh.executeHiveSql();
	//step1
	ddfh.setHivesql("create table table_1 as select deviceid ,dt,lead_speed,lag_speed,"
			+ " case when (lead_speed <>0) then 'e' when (lag_speed<>0) then 's' end status"
			+ " from (select deviceid, dt ,LAG(speed,1,0) over (partition by deviceid order by dt) as lag_speed,"
			+ " LEAD(speed,1,0) over (partition by deviceid order by dt) as lead_speed from t_data101_720 ) a");
	ddfh.executeHiveSql();
	//step2
	
	ddfh.setHivesql("create table table_2 as select deviceid,dt,lead_datetime "
			+ " from(select deviceid ,dt,lead(dt,1,null) over (partition by deviceid order by dt) as lead_datetime,status "
			+ " from table_1 where status ='s' or status ='e') a where  unix_timestamp(lead_datetime)-unix_timestamp(dt)>10  and status ='s'");
	ddfh.executeHiveSql();
	//setp3
	ddfh.setHivesql("create table table_3 as select a.* from t_data101_720 a join table_2 b where a.dt between b.dt and  b.lead_datetime "
			+ " and a.deviceid=b.deviceid");
	ddfh.executeHiveSql();
	
	//step4
	ddfh.setHivesql("create table table_4 as select deviceid ,percentile( cast(ax*1000 as bigint),0.5)/1000 as ax ,"
			+ "percentile(cast(ay*1000 as bigint),0.5)/1000 as ay ,-1*percentile(cast(az*1000 as bigint),0.5)/1000 as az  from table_3 group by deviceid");
	ddfh.executeHiveSql();
	
	//step5
	ddfh.setHivesql("create table table_5 as select  deviceid,sqrt(ax*ax+ay*ay) as ra2 ,sqrt(ax*ax+ay*ay+az*az) as ra3 ,ax/sqrt(ax*ax+ay*ay) as cx,"
			+ " ay/sqrt(ax*ax+ay*ay) as sx,az/sqrt(ax*ax+ay*ay+az*az) as cz,sqrt(ax*ax+ay*ay)/sqrt(ax*ax+ay*ay+az*az) as sz from table_4");
	ddfh.executeHiveSql();
	
	//setp6
	ddfh.setHivesql("create table table_6 as select deviceid ,cx*cz as a1,sx*cz as a2,-sz as a3, "
			+ "-sx as b1,cx as b2,0 as b3,cx*sz as c1,sx*sz as c2,cz as c3 from table_5");
	ddfh.executeHiveSql();
	
	//setp7
	ddfh.setHivesql("create table table_7 as select a.deviceid,a.dt,speed,wx,wy,wz, a1*ax+a2*ay-a3*az as ax ,b1*ax+b2*ay-b3*az as ay,"
			+ " (c1*ax+c2*ay-c3*az) az from t_data101_720 a join table_6 b on a.deviceid=b.deviceid  ");
	ddfh.executeHiveSql();
	
	//setp8
	ddfh.setHivesql("create table table_8 as select deviceid,dt, wx,wy,wz, ax,ay,az,speed,LAG(speed,1,0) over (partition by deviceid order by dt) as lag_speed,"
			+ " LEAD(speed,1,0) over (partition by deviceid order by dt) as lead_speed from table_7 ");
	ddfh.executeHiveSql();
	
	
	//setp9
	ddfh.setHivesql("create table table_9 as select deviceid,dt, case when  ax<0 and ay>0 then 180+atan(ay/ax)*180/3.14  "
				+ " when  ax<0 and ay<0 then -180+atan(ay/ax)*180/3.14 else atan(ay/ax)*180/3.14 end  as theta3  "
				+ "from (select deviceid,dt, wx,wy,wz, ax,ay,az,speed from table_8 where abs(wx)+abs(wy)+abs(wz)<0.03  "
				+ "and speed-lag_speed>5 and lead_speed-speed>5 )a");
	ddfh.executeHiveSql();
	
	//setp10
	ddfh.setHivesql("create table table_10 as select deviceid,percentile(cast(theta3*1000 as bigint),0.5)/1000 as theta3 from table_9 group by deviceid ");
	ddfh.executeHiveSql();	
	
	//setp11
	ddfh.setHivesql("create table table_11 as select deviceid,cos(theta3*3.14/180) as a1 ,sin(theta3*3.14/180) as a2,0 as a3, "
			+ " -sin(theta3*3.14/180) as b1,cos(theta3*3.14/180) as b2 ,0 as b3 ,0 as c1 ,0 as c2 ,1 as c3 from table_10");
	ddfh.executeHiveSql();		
	

	//setp12
	ddfh.setHivesql("create table table_12 as select a.deviceid,a.a1*b.a1+a.a2*b.b1+a.a3*b.c1 as a1,a.a1*b.a2+a.a2*b.b2+a.a3*b.c2 as a2,"
			+ "a.a1*b.a3+a.a2*b.b3+a.a3*b.c3 as a3,a.b1*b.a1+a.b2*b.b1+a.b3*b.c1 as b1,a.b1*b.a2+a.b2*b.b2+a.b3*b.c2 as b2,"
			+ "a.b1*b.a3+a.b2*b.b3+a.b3*b.c3 as b3,a.c1*b.a1+a.c2*b.b1+a.c3*b.c1 as c1,a.c1*b.a2+a.c2*b.b2+a.c3*b.c2 as c2,"
			+ "a.c1*b.a3+a.c2*b.b3+a.c3*b.c3 as c3 from table_11 a, table_6 b where a.deviceid=b.deviceid");
	ddfh.executeHiveSql();		
	
	
	
	
	
	
	
	
	
	
	
////	ddfh.getPath();
//	ddfh.setHivesql("create table tmp_reportdata101 as select deviceid ,max(speed) maxspeed ,avg(speed) as avgspeed,min(speed) as minspeed from t_reportData101 group by deviceid");
//	ddfh.executeHiveSql();
	ddfh.setHivesql("insert overwrite local directory '"+ddfh.path+"' row format delimited fields terminated by '\\t'select * from table_12");
	ddfh.executeHiveSql();
	System.out.println(ddfh.getFilePath());
//	// select max(speed) maxspeed ,avg(speed) as avgspeed,min(speed) as
//	// minspeed from t_reportData101;
//	// ddfh.setHivesql("select count(*) from t_test_1");
//	// ddfh.executeHiveSql();
//	// ddfh.close();
//	ddfh.setMysql("load data local infile '"+ddfh.getFilePath()+"' replace into table t_reportdata101 fields terminated by '\t'");
////	ddfh.setMysql("load data local infile '"+"F:/i.txt"+"' replace into table t_reportdata101 fields terminated by '\t'");
//	ddfh.executeMysql();
	ddfh.close();
	System.out.println("compete");
	// System.out.println(ddfh.getFilePath(new File("F:/hive")));
}
}
