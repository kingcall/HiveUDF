package com.kingcall.bigdata.HiveUDF;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kingcall.bigdata.HiveUtil.HttpClientUtilThread;
import org.apache.hadoop.hive.ql.exec.Description;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Description(name = "vocana", value = "_FUNC_(" +
        "String dataSource 数据源(维修三包、长安百科-视频、NPS调研等)\n" +
        "List domainList : 不同指标表（如汽车、VRT）\n" +
        "String title: 主题或者问题或者目录名称或者活动名称（随数据源而变化）\n" +
        "String text: 正文或者回答或者评分或者评论（随数据源而变化）\n" +
        "Int position: position=1分析title和text，position不为1只分析text\n" +
        "int textType: 分析数据类型（用1、2、3表示，具体对应类型看下面例子）\n" +
        "String populationGroup: 人群（贬损者、推荐者、中立者）\n" +
        "Sting productName:产品名称\n" +
        ") - return result",
        extended = ""
)
public class PublicOpinionAnalysisUDFThread extends GenericUDF {
    private ListObjectInspector listoi;
    private MapObjectInspector mapOI;
    private ThreadPoolExecutor threadPool;
    CountDownLatch latch;
    RejectedExecutionHandler handler;
    AtomicInteger counter;
    private List<String> urls=new ArrayList<>();
    private static Logger logger=LoggerFactory.getLogger(PublicOpinionAnalysisUDFThread.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        urls.add("http://10.64.22.126:8110/changan/voc/analyzer");
        urls.add("http://10.64.22.125:8110/changan/voc/analyzer");
//        urls.add("http://10.64.22.125:8100/changan/voc/analyzer");
//        urls.add("http://10.64.22.126:8100/changan/voc/analyzer");
//        urls.add("http://10.64.22.127:8100/changan/voc/analyzer");
//        urls.add("http://10.64.23.36:8100/changan/voc/analyzer");
//        urls.add("http://10.64.23.37:8100/changan/voc/analyzer");
        /**
         * 因为是通用接口，所以参数个数是固定的
         */
        if (arguments.length != 1) {
            throw new UDFArgumentException("parameter count is not one,jsut one parameters is need");
        }

        listoi = (ListObjectInspector) arguments[0];
        mapOI = (MapObjectInspector) listoi.getListElementObjectInspector();

        handler=new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.error(String.format("有任务抛弃 ,core:%d active:%d queue:%d completed:%d taskCount:%d",
                        executor.getCorePoolSize(),
                        executor.getActiveCount(),
                        executor.getQueue().size(),
                        executor.getCompletedTaskCount(),
                        executor.getTaskCount()

                ));
            }
        };

        threadPool = new ThreadPoolExecutor(
                300,
                800,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(4000),
                new ThreadFactoryBuilder().setNameFormat("job-task-%d").build(),
                handler
        );

        counter=new AtomicInteger();

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
        ArrayBlockingQueue<Object[]> arrayBlockingQueue = new ArrayBlockingQueue<>(nelements);
        //遍历list 准备参数
        for (int i = 0; i < nelements; i++) {
            //获取map
            Map<String, String> parameterMap = new HashMap<>();
            // 这里应该有更好的方法的，但是没找到，只能遍历
            mapOI.getMap(listoi.getListElement(arguments[0].get(), i)).forEach((k, v) -> {
                parameterMap.put(k.toString(), v.toString());
            });
            String url = urls.get(i % urls.size());
            // 在这里开多线程 使用线程池处理
            threadPool.execute(() -> {
                try {
                    // 定义返回的结构体
                    Object[] struct = new Object[4];
                    // 这里需要获取dataid
                    String dataId = parameterMap.get("dataId");
                    //String url=urls.get(0);
                    String result = HttpClientUtilThread.doPost(url, JSONObject.parseObject(JSON.toJSONString(parameterMap)));
                    // 认为返回结果正常开始处理
                    if (result != null && !"".equals(result)) {

                        JSONObject dataJSONObject = JSONObject.parseObject(result);
                        // extractedInfo
                        JSONObject jsonDataObj = dataJSONObject.getJSONObject("data");
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
                        // msg
                        String msg = dataJSONObject.getString("msg");
                        // 时长
                        String time = dataJSONObject.getString("time");

                        // 为返回的结构体赋值
                        struct[0] = dataId;
                        struct[1] = maps;
                        struct[2] = msg;
                        struct[3] = time;
                        // 把返回值加入到队列 准备返回
                        arrayBlockingQueue.put(struct);
                        if (!"200".equals(msg)) {
                            counter.getAndIncrement();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        taskInfo("submited",start, nelements,0);
        // 等待批次数据处理完毕
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        taskInfo("completed",start, nelements,counter.getAndSet(0));
        return arrayBlockingQueue.toArray();

    }

    public void taskInfo(String message,long start,int nelements,int timeoutCnt){
        logger.warn(
                String.format("%s taskinfo: core:%d pool:%d active:%d queued:%d completed:%d taskCount:%d time:%d batchCount:%d timeoutCnt:%d",
                        message,
                        threadPool.getCorePoolSize(),
                        threadPool.getPoolSize(),
                        threadPool.getActiveCount(),
                        threadPool.getQueue().size(),
                        threadPool.getCompletedTaskCount(),
                        threadPool.getTaskCount(),
                        System.currentTimeMillis() - start,
                        nelements,
                        timeoutCnt
                ));
    }


    @Test
    public void testPublicOpinionAnalysisUDF() throws HiveException {
        PublicOpinionAnalysisUDFThread udf = new PublicOpinionAnalysisUDFThread();
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
