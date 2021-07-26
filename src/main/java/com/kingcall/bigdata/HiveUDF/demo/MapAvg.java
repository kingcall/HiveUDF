package com.kingcall.bigdata.HiveUDF.demo;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MapAvg extends GenericUDF {

    private transient MapObjectInspector mapObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        mapObjectInspector = (MapObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        double result = mapObjectInspector.getMap(o).values().stream().mapToDouble(line -> Double.parseDouble(((Object) line).toString())).average().getAsDouble();
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
