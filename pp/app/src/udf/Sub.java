package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
public class Sub extends UDF{
	public String evaluate(String a,Integer b){		
		return a.substring(0, b);		
	}
}
