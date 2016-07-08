package com.naresh.org.udf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ArrayUpdate extends UDF
{

	public int evaluate(ArrayList<String> args,String operationtype)
	{
		//convert string array list to array of ints
		List<Integer> numberslist = new ArrayList<Integer>();
		for(String arg: args)
		{
		numberslist.add(new Integer(arg));	
		}
		int result=0;
		//Calculate sum
		if(operationtype.equalsIgnoreCase("sum")||operationtype.equalsIgnoreCase("mean"))
		{
			for(Integer value:numberslist)
				result = result+value;
		}
		if(operationtype.equalsIgnoreCase("sum"))
			return result;
		//Calculate mean
		if(operationtype.equalsIgnoreCase("mean"))
		{
			return (int)(result/(numberslist.size()));
		}
		//Calculate medium
		if(operationtype.equalsIgnoreCase("medium"))
		{
			Collections.sort(numberslist);
			if(numberslist.size()%2!=0)
			{
				result  = numberslist.get((numberslist.size()/2)+1);
			}
			else
			{
				result = (numberslist.get((numberslist.size()/2)+1)+numberslist.get(numberslist.size()/2))/2;
			}
						
		}
		//Calculate Mode
		int modevalue=0;
		int modevaluecount=0;
		if(operationtype.equalsIgnoreCase("mode"))
		{
			Map<Integer,Integer> countmap =new HashMap<Integer,Integer>();
			for(Integer value:numberslist)
			{
				int count=0;
				if(countmap.containsKey(value))
				{
					 count= countmap.get(value)+1;
					countmap.put(value, count);
				}
				else
				{
					countmap.put(value, 1);
					count=1;
				}
				if(count>modevaluecount)
				{
					modevaluecount = count;
					modevalue = value;
				}
			}
			result = modevalue;
		}
		
		return result;
		
	}
	
	
}
