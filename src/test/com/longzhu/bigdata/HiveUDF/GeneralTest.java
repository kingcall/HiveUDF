package com.longzhu.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created by plu on 2017/12/15.
 */
public class GeneralTest {

    @Test
    public void Test() throws HiveException  {
        AnsjSeg udf = new AnsjSeg();

        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
         ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] init_args = {valueOI0, valueOI1};
        udf.initialize(init_args);

        Text str = new Text("我是测试字符串");
        ArrayList<Object> exp = new ArrayList<Object>();
        exp.add("测试");
        exp.add("字符串");

        DeferredObject valueObj0 = new DeferredJavaObject(str);
        DeferredObject valueObj1 = new DeferredJavaObject(1);
        DeferredObject[] args = { valueObj0, valueObj1 };
        ArrayList<Object> res = (ArrayList<Object>) udf.evaluate(args);
        assertTrue("ansj_seg() test: ", exp.equals(res));

        /*
        Long2IP testObj = new Long2IP();
        LongWritable ip = new LongWritable(1344653710L);
        IntWritable inv = new IntWritable(0);
        Text res = testObj.evaluate(ip, inv);
        assertEquals("80.37.201.142", res.toString());*/
    }
}