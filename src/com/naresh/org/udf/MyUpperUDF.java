package com.naresh.org;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "to_upper_convert", value = "_FUNC_(String) - returns Uppercase of the given String \n"
		+ "OR _FUNC_(String,String) - concats 2 strings,Reverse them and returns uppercase", extended = "Example: \n"
		+ ">SELECT _FUNC_('Prasad') from dual; \n"
		+ "PRASAD"
		+ "\n >SELECT _FUNC_('Prasad','Hadoop') from dual; \n" + "POODAHDASARP")
public final class MyUpperUDF extends UDF {

	public MyUpperUDF() {
		System.out.println("MyUpperUDF()");
	}

	public String evaluate(String data) {
		System.out.println("MyUpperUDF.evaluate(-)");
		return data.toUpperCase();
	}

	// Merge two String
	// Reverse them
	// convert to upper case
	// return it back
	public String evaluate(String data1, Text data2) {
		System.out.println("MyUpperUDF.evaluate(-,-)");
		StringBuffer sb = new StringBuffer();
		sb.append(data1);
		sb.append(data2.toString());
		sb.reverse();

		String str = sb.toString();
		return str.toUpperCase();

	}

}
