package app;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.HiveParser.sysFuncNames_return;

public class Demo2 {
		public static void main(String[] args) throws IOException {
			Configuration conf= new Configuration();
			conf.set("fs.defaultFS", "hdfs://210.51.31.67:9000");
			FileSystem fs =FileSystem.get(conf);
			FileStatus[] fss=fs.listStatus(new Path("/"));
			for(FileStatus f :fss){
				System.out.println(f.getPath().getName());
			}
		}
}
