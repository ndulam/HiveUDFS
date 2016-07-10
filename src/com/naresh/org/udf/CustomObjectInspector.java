package com.naresh.org.udf;

import java.util.ArrayList;
import java.util.List;

import com.naresh.org.udf.Utils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

public class CustomObjectInspector {
    private PrimitiveObjectInspector inspector;
    
    public CustomObjectInspector(ObjectInspector arg) {
	inspector = (PrimitiveObjectInspector) arg;
    }

    public boolean sameAsTypeIn(ListObjectInspector loi) {
	PrimitiveObjectInspector poi = (PrimitiveObjectInspector) loi.getListElementObjectInspector();
	return poi.getPrimitiveCategory() == getCategory();
    }

    public PrimitiveCategory getCategory() {
	return inspector.getPrimitiveCategory();
    }

    public AbstractPrimitiveJavaObjectInspector getAnInspector() {
	return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(getCategory());
    }

    public Object get(Object value) {
	return inspector.getPrimitiveJavaObject(value);
    }

    public float toFloat(Object value) {
	float result;
	value = get(value);
	switch(getCategory()) {
	case STRING:
	    long timestamp = Utils.stringToTimestamp((String) value);
	    result = (timestamp < 0) ? 0L : (new Long(timestamp)).floatValue();
	    break;
	case BOOLEAN:
	    result = ((Boolean) value).booleanValue() ? 1.0f : 0.0f;
	    break;
	case UNKNOWN:
	    result = 0.0f;
	    break;
	case VOID:
	    result = 0.0f;
	    break;
	default: // all other types are numerical
	    result = ((Number) value).floatValue();
	    break;
	}
	return result;
    }

    public long toLong(Object value) {
	long result;
	value = get(value);
	switch(getCategory()) {
	case STRING:
	    long timestamp = Utils.stringToTimestamp((String) value);
	    result = (timestamp < 0) ? 0L : (new Long(timestamp)).longValue();
	    break;
	case BOOLEAN:
	    result = ((Boolean) value).booleanValue() ? 1L : 0L;
	    break;
	case UNKNOWN:
	    result = 0L;
	    break;
	case VOID:
	    result = 0L;
	    break;
	default: // all other types are numerical
	    result = ((Number) value).longValue();
	    break;
	}
	return result;
    }

    public boolean isNull() {
	return getCategory() == PrimitiveCategory.VOID;
    }

    public boolean isString() {
	return getCategory() == PrimitiveCategory.STRING;
    }

    public static boolean isPrimitive(ObjectInspector oi) {
	return oi.getCategory() == ObjectInspector.Category.PRIMITIVE;
    }

    public static boolean isList(ObjectInspector oi) {
	return oi.getCategory() == ObjectInspector.Category.LIST;
    }

    public boolean equalPrimitive(Object first, Object second) {
	if((first == null && second != null) || (first != null && second == null))
	    return false;
	if(first == null && second == null)
	    return true;

	first = get(first);
	second = get(second);
	boolean result;
	switch(getCategory()) {
	case STRING:
	    result = ((String) first).compareTo((String) second) == 0;
	    break;
	case BOOLEAN:
	    result = ((Boolean) first).equals((Boolean) second);
	    break;
	case UNKNOWN:
	    result = first.equals(second);
	    break;
	case VOID:
	    result = first.equals(second);
	    break;
	default: // all other types are numerical
	    result = ((Integer) first).intValue() == ((Integer) second).intValue();
	    break;
	}
	return result;
    }
}