package com.kingcall.bigdata.HiveUDF;


/**
 * 统计分数
 * hive> describe tb_test2;
 * OK
 * name    string
 * score_list  array<map<string,int>>
 * Time taken: 0.074 seconds
 * hive> select * from tb_test2;
 * OK
 * A   [{"math":100,"english":90,"history":85}]
 * B   [{"math":95,"english":80,"history":100}]
 * C   [{"math":80,"english":90,"histroy":100}]
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import java.util.ArrayList;

public class HelloGenericUDFNew extends GenericUDF {
    //输入变量定义
    private ObjectInspector nameObj;
    private ListObjectInspector listoi;
    private MapObjectInspector mapOI;
    private ArrayList<Object> valueList = new ArrayList<Object>();
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        nameObj = (ObjectInspector)arguments[0];
        listoi = (ListObjectInspector)arguments[1];
        mapOI = ((MapObjectInspector)listoi.getListElementObjectInspector());
        //输出结构体定义
        ArrayList structFieldNames = new ArrayList();
        ArrayList structFieldObjectInspectors = new ArrayList();
        structFieldNames.add("name");
        structFieldNames.add("totalScore");

        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );
        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableIntObjectInspector );

        StructObjectInspector si2;
        si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
        return si2;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException{
        LazyString LName = (LazyString)(arguments[0].get());
        String strName = ((StringObjectInspector)nameObj).getPrimitiveJavaObject( LName );

        int nelements = listoi.getListLength(arguments[1].get());
        int nTotalScore=0;
        valueList.clear();
        //遍历list
        for(int i=0;i<nelements;i++)
        {
            LazyMap LMap = (LazyMap)listoi.getListElement(arguments[1].get(),i);
            //获取map中的所有value值
            valueList.addAll(mapOI.getMap(LMap).values());
            for (int j = 0; j < valueList.size(); j++)
            {
                nTotalScore+=Integer.parseInt(valueList.get(j).toString());
            }
        }
        Object[] e;
        e = new Object[2];
        e[0] = new Text(strName);
        e[1] = new IntWritable(nTotalScore);
        return e;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert( children.length>0 );

        StringBuilder sb = new StringBuilder();
        sb.append("helloGenericUDFNew(");
        sb.append(children[0]);
        sb.append(")");

        return sb.toString();
    }
}
