package com.yobdcdoll.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MovieUDTF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors == null || objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("MovieUDTF takes only one argument");
        }
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("MovieUDTF takes string as a parameter");
        }
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("genre");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String[] split = deferredObjects[0].get().toString().split("|");
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
