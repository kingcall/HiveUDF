package com.kingcall.bigdata.HiveUDF;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.kingcall.bigdata.HiveUtil.HttpClientUtilBatch;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


public class PublicOpinionAnalysisUDF extends GenericUDF {
    private ListObjectInspector listoi;
    private MapObjectInspector mapOI;
    private ThreadPoolExecutor threadPool;
    CountDownLatch latch;
    RejectedExecutionHandler handler;
    AtomicInteger counter;
    private List<String> urls=new ArrayList<>();
    private static Logger logger=LoggerFactory.getLogger(PublicOpinionAnalysisUDF.class);
    String url = "http://10.64.22.126:8110/changan/voc/analyzerList";

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        /**
         * 因为是通用接口，所以参数个数是固定的
         */
        if (arguments.length != 1) {
            throw new UDFArgumentException("parameter count is not one,jsut one parameters is need");
        }

        listoi = (ListObjectInspector) arguments[0];
        mapOI = (MapObjectInspector) listoi.getListElementObjectInspector();


        //输出结构体定义
        AbstractPrimitiveJavaObjectInspector primitiveStringInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        //extractedInfo 的定义
        StandardMapObjectInspector standardMapObjectInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                primitiveStringInspector,
                primitiveStringInspector

        );
        StandardListObjectInspector listObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(standardMapObjectInspector);

        ArrayList structFieldNames = new ArrayList();
        ArrayList structFieldObjectInspectors = new ArrayList();
        // 字段定义
        structFieldNames.add("dataid");
        structFieldNames.add("extractedInfo");
        structFieldNames.add("msg");
        structFieldNames.add("time");
        // 类型定义
        structFieldObjectInspectors.add(primitiveStringInspector);
        structFieldObjectInspectors.add(listObjectInspector);
        structFieldObjectInspectors.add(primitiveStringInspector);
        structFieldObjectInspectors.add(primitiveStringInspector);

        StructObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);

        return ObjectInspectorFactory.getStandardListObjectInspector(structObjectInspector);

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 其实线程池可以放在这里创建 指定参数
        int nelements = listoi.getListLength(arguments[0].get());
        long start = System.currentTimeMillis();
        latch = new CountDownLatch(nelements);
        // 返回结果的队列
        List<Object[]> arrayList = new ArrayList<Object[]>(nelements);
        //遍历list 准备参数
        JSONArray jsonArrayParameter = new JSONArray();
        for (int i = 0; i < nelements; i++) {
            //获取map
            JSONObject parameterJson = new JSONObject();
            // 这里应该有更好的方法的，但是没找到，只能遍历
            mapOI.getMap(listoi.getListElement(arguments[0].get(), i)).forEach((k, v) -> {
                parameterJson.put(k.toString(), v.toString());
            });
            jsonArrayParameter.add(parameterJson);
        }

        JSONArray  jsonArrayResults = HttpClientUtilBatch.doPost(url, jsonArrayParameter);
        for (int i = 0; i < jsonArrayResults.size(); i++) {
            JSONObject jsonObject = jsonArrayResults.getJSONObject(i);
            if (jsonObject!=null){
                Object[] struct = new Object[4];
                String dataId = jsonObject.getString("dataId");
                String msg = jsonObject.getString("msg");
                String time = jsonObject.getString("costTime");
                //
                JSONObject jsonDataObj = jsonObject.getJSONObject("data");
                ArrayList<Map<String, String>> maps = new ArrayList<>();
                if (jsonDataObj!=null){
                    JSONArray jsonArray=jsonDataObj.getJSONArray("extractedInfo");
                    if (jsonArray!=null && jsonArray.size()>0){
                        maps = new ArrayList<>(jsonArray.size());
                        for (int j = 0; j < jsonArray.size(); j++) {
                            Map<String, String> innerMap = JSONObject.parseObject(jsonArray.getJSONObject(j).toJSONString(), new TypeReference<Map<String, String>>() {
                            });
                            maps.add(innerMap);
                        }
                    }
                }
                struct[0] = dataId;
                struct[1] = maps;
                struct[2] = msg;
                struct[3] = time;
                arrayList.add(struct);
            }
        }
        logger.warn(String.format("input bacth count:%d result count:%d",nelements,jsonArrayResults.size() ));
        return arrayList;
    }


    @Test
    public void testPublicOpinionAnalysisUDF() throws HiveException {
        PublicOpinionAnalysisUDF udf = new PublicOpinionAnalysisUDF();
        StandardMapObjectInspector standardMapObjectInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector

        );
        StandardListObjectInspector listObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(standardMapObjectInspector);


        ObjectInspector[] init_args = {listObjectInspector};
        udf.initialize(init_args);

        ArrayList<Map<Text, Text>> arrayList = new ArrayList<>();
        HashMap<Text, Text> map = new HashMap<>();
        map.put(new Text("dataSource"), new Text("长安汽车直评"));
        map.put(new Text("dataId"), new Text("2"));
        map.put(new Text("'domainListJson'"), new Text("[\"VRT\",\"全旅程客户\",\"全领域业务\",\"商品化属性\",\"销售线索\"]"));
        arrayList.add(map);
        arrayList.add(map);
        arrayList.add(map);

        DeferredObject[] args = {
                new DeferredJavaObject(arrayList)
        };
        udf.evaluate(args);
        arrayList.add(map);
        arrayList.add(map);
        arrayList.add(map);
        DeferredObject[] args2 = {new DeferredJavaObject(arrayList)};
        udf.evaluate(args);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
