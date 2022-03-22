package com.kingcall.bigdata.HiveUDF;


import com.alibaba.fastjson.JSONObject;
import com.kingcall.bigdata.HiveUtil.HttpClientUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

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
        extended = "")

public class PublicOpinionAnalysisUDF extends GenericUDF {
    private Converter converter;
    private String url;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        /**
         * 因为是通用接口，所以参数个数是固定的
         */
        if (arguments.length < 8) {
            throw new UDFArgumentException("parameter is not enough,8 parameters is need");
        }
        converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        url="http://10.64.22.151:8100/changan/voc/analyze";
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        /**
         * 根据接口的调用情况解析参数
         * String dataSource 数据源(维修三包、长安百科-视频、NPS调研等)
         * List domainList : 不同指标表（如汽车、VRT）
         * String title: 主题或者问题或者目录名称或者活动名称（随数据源而变化）
         * String text: 正文或者回答或者评分或者评论（随数据源而变化）
         * Int position: position=1分析title和text，position不为1只分析text
         * int textType: 分析数据类型（用1、2、3表示，具体对应类型看下面例子）
         * String populationGroup: 人群（贬损者、推荐者、中立者）
         * Sting productName:产品名称
         */
        String dataSource =converter.convert(arguments[0].get()).toString();
        List<String> domainList = Arrays.asList(converter.convert(arguments[1].get()).toString().split(","));
        String title = converter.convert(arguments[2].get()).toString();
        String text = converter.convert(arguments[3].get()).toString();
        String position = converter.convert(arguments[4].get()).toString();
        String textType = converter.convert(arguments[5].get()).toString();
        String populationGroup =  converter.convert(arguments[6].get()).toString();
        String productName = converter.convert(arguments[7].get()).toString();
        // 构建参数 调用HTTP接口
        JSONObject jsObject = new JSONObject();
        jsObject.put("dataSource", dataSource);
        jsObject.put("domainList", domainList);
        jsObject.put("title", title);
        jsObject.put("text", text);
        jsObject.put("position", position);
        jsObject.put("populationGroup", populationGroup);
        jsObject.put("productName", productName);

        return HttpClientUtil.doPost(url, jsObject);

    }

    @Test
    public void testPublicOpinionAnalysisUDF() throws HiveException {
        PublicOpinionAnalysisUDF udf = new PublicOpinionAnalysisUDF();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] init_args = {valueOI0};
        udf.initialize(init_args);

        Text str = new Text("我是测试字符串2");
        DeferredObject[] args = {
                new DeferredJavaObject(str),
                new DeferredJavaObject(str),
                new DeferredJavaObject(str),
                new DeferredJavaObject(str),
                new DeferredJavaObject(str),
                new DeferredJavaObject(str),
               // new DeferredJavaObject(str),
                new DeferredJavaObject(str)
        };
        String res = (String) udf.evaluate(args);
        System.out.println(res);
    }


    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
