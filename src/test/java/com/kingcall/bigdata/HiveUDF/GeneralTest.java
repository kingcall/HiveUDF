package com.kingcall.bigdata.HiveUDF;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by plu on 2017/12/15.
 */
public class GeneralTest {

    @Test
    public void Test() throws HiveException {
//        AnsjSeg udf = new AnsjSeg();
//
//        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
//        ObjectInspector[] init_args = {valueOI0, valueOI1};
//        udf.initialize(init_args);
//
//        Text str = new Text("我是测试字符串");
//        ArrayList<Object> exp = new ArrayList<Object>();
//        exp.add("测试");
//        exp.add("字符串");
//
//        DeferredObject valueObj0 = new DeferredJavaObject(str);
//        DeferredObject valueObj1 = new DeferredJavaObject(1);
//        DeferredObject[] args = {valueObj0, valueObj1};
//        ArrayList<Object> res = (ArrayList<Object>) udf.evaluate(args);
//        assertTrue("ansj_seg() test: ", exp.equals(res));

        Long2IP testObj = new Long2IP();
        LongWritable ip = new LongWritable(1344653710L);
        IntWritable inv = new IntWritable(0);
        Text reText = testObj.evaluate(ip, inv);
        System.out.println(reText.toString());
        assertEquals("80.37.201.142", reText.toString());

    }

    @Test
    public void testAnsjSeg() {
        String str = "我叫李太白，我是一个诗人，我生活在唐朝";
        // 选择使用哪种分词器 BaseAnalysis ToAnalysis NlpAnalysis  IndexAnalysis
        Result result = ToAnalysis.parse(str);
        System.out.println(result);
        KeyWordComputer kwc = new KeyWordComputer(5);
        // 算了一下每个词的得分情况
        Collection<Keyword> keywords = kwc.computeArticleTfidf(str);
        System.out.println(keywords);
    }

    @Test
    public void testIk() {
        String keyword = "我叫李太白，我是一个诗人，我生活在唐朝";
        boolean smart = true;
        StringReader reader = new StringReader(keyword);
        IKSegmenter iks = new IKSegmenter(reader, smart);
        StringBuilder buffer = new StringBuilder();
        try {
            Lexeme lexeme;
            while ((lexeme = iks.next()) != null) {
                buffer.append(lexeme.getLexemeText()).append(',');
            }
        } catch (IOException e) {
        }
        //去除最后一个,
        if (buffer.length() > 0) {
            buffer.setLength(buffer.length() - 1);
        }
        System.out.println(buffer.toString());
        ;
    }


    @Test
    public void testAnsjSegFunc() throws HiveException {
        AnsjSeg udf = new AnsjSeg();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] init_args = {valueOI0, valueOI1};
        udf.initialize(init_args);

        Text str = new Text("我是测试字符串");
        ArrayList<Object> exp = new ArrayList<Object>();
        exp.add("测试");
        exp.add("字符串");

        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(str);
        GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(0);
        GenericUDF.DeferredObject[] args = {valueObj0, valueObj1};
        ArrayList<Object> res = (ArrayList<Object>) udf.evaluate(args);
        System.out.println(res);
    }


    @Test
    public void testIkSegFunc() throws HiveException {
        IknalyzerSeg udf = new IknalyzerSeg();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector[] init_args = {valueOI0, valueOI1};
        udf.initialize(init_args);

        Text str = new Text("我是测试字符串");
        ArrayList<Object> exp = new ArrayList<Object>();
        exp.add("测试");
        exp.add("字符串");

        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(str);
        GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(1);
        GenericUDF.DeferredObject[] args = {valueObj0, valueObj1};
        ArrayList<Object> res = (ArrayList<Object>) udf.evaluate(args);
        System.out.println(res);
    }

    @Test
    public void ip2Region() throws HiveException {
        Ip2Region udf = new Ip2Region();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] init_args = {valueOI0};
        udf.initialize(init_args);
        String ip = "220.248.12.158";

        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(ip);

        GenericUDF.DeferredObject[] args = {valueObj0};
        Text res = (Text) udf.evaluate(args);
        System.out.println(res.toString());
    }

}
