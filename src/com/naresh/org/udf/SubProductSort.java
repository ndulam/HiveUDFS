package com.naresh.org.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser.fileFormat_return;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class SubProductSort extends GenericUDF	
{

	ListObjectInspector arrayOI = null;
	StringObjectInspector strOI=null;
	StructObjectInspector structOI=null;
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException 
	{
		System.out.println("Inside evaluate");
		//List<?> value_array = arrayOI.getList(arg0[0].get());
		ArrayList value_array = new ArrayList(arrayOI.getList(arg0[0].get()));
		System.out.println(1);
		String property =strOI.getPrimitiveJavaObject(arg0[1].get());
		System.out.println(2);
		System.out.print(property);
		Collections.sort(value_array,new myCustomComperator(property));
		System.out.println(value_array);	
		//System.out.println(value_array.getClass());
		System.out.println(3);
		return value_array;
	}

	@Override
	public String getDisplayString(String[] arg0) 
	{
	
		return "Sort the sub products depends on the property field";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException 
	{

		System.out.println("Inside initialize ");
		if(arg0.length<2)
		{
			throw new UDFArgumentException("please provide two arguments");
		}
		arrayOI =  (ListObjectInspector) arg0[0];
		strOI = (StringObjectInspector) arg0[1];
		structOI = (StructObjectInspector) arrayOI.getListElementObjectInspector();
		return arrayOI;
	}

	public class myCustomComperator implements Comparator
	{

		String name;
		 StructField field;
		myCustomComperator(String name)
		{
			field = structOI.getStructFieldRef(name);
		}
		@Override
		public int compare(Object obj1, Object obj2) 
		{
			System.out.println("compa"+1);
			Object f1 = structOI.getStructFieldData(obj1, field);
			System.out.println(f1);
			System.out.println("compa"+2);
	        Object f2 = structOI.getStructFieldData(obj2, field);
	        System.out.println(f2);
	        System.out.println("compa"+3);
	        // compare using hive's utility functions
	        int temp=ObjectInspectorUtils.compare(f1, field.getFieldObjectInspector(), 
		            f2, field.getFieldObjectInspector());
	        System.out.println(temp);
	        return temp;
	        
			/*StructObjectInspector sob1 = (StructObjectInspector)obj1;
			StructObjectInspector sob2 = (StructObjectInspector)obj2;
			List<? extends StructField> fields1 = sob1.getAllStructFieldRefs();
			List<? extends StructField> fields2 = sob2.getAllStructFieldRefs();
			String temp1="";
			String temp2="";
			int length = fields1.size();
			 for(int i = 0; i < length; i++) {
			        StructField field = fields1.get(i);
			        if(field.getFieldName().equalsIgnoreCase(name))
			        {
			        	temp1 = field.getFieldComment();
			        	break;
			        }
			 }
				length = fields2.size();
				 for(int i = 0; i < length; i++) {
				        StructField field = fields2.get(i);
				        if(field.getFieldName().equalsIgnoreCase("name"))
				        {
				        	temp2 = field.getFieldComment();
				        	break;
				        }
				 }
			return temp1.compareTo(temp2);*/
		}
		
		
	}
	
}
