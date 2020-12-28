package com.kingcall.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Description(name = "ansj_seg", value = "_FUNC_(str) - chinese words segment using Iknalyzer. Return list of words.",
        extended = "Example: select _FUNC_('我是测试字符串') from src limit 1;\n"
                + "[\"我\", \"是\", \"测试\", \"字符串\"]")
public class IknalyzerSeg extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    //用来存放停用词的集合
    Set<String> stopWordSet = new HashSet<String>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1 || arguments.length > 2) {
            throw new UDFArgumentLengthException(
                    "The function AnsjSeg(str) takes 1 or 2 arguments.");
        }
        //读入停用词文件
        BufferedReader StopWordFileBr = null;
        try {
            StopWordFileBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File("stopwords/baidu_stopwords.txt"))));
            //初如化停用词集
            String stopWord = null;
            for(; (stopWord = StopWordFileBr.readLine()) != null;){
                stopWordSet.add(stopWord);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        converters[0] = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        if (2 == arguments.length) {
            converters[1] = ObjectInspectorConverters.getConverter(arguments[1], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        boolean filterStop = false;
        if (arguments[0].get() == null) {
            return null;
        }
        if (2 == arguments.length) {
            IntWritable filterParam = (IntWritable) converters[1].convert(arguments[1].get());
            if (1 == filterParam.get()) filterStop = true;
        }
        Text s = (Text) converters[0].convert(arguments[0].get());
        StringReader reader = new StringReader(s.toString());
        IKSegmenter iks = new IKSegmenter(reader, true);
        List<Text> list = new ArrayList<>();
        if (filterStop) {
            try {
                Lexeme lexeme;
                while ((lexeme = iks.next()) != null) {
                    if (!stopWordSet.contains(lexeme.getLexemeText())) {
                        list.add(new Text(lexeme.getLexemeText()));
                    }
                }
            } catch (IOException e) {
            }
        } else {
            try {
                Lexeme lexeme;
                while ((lexeme = iks.next()) != null) {
                    list.add(new Text(lexeme.getLexemeText()));
                }
            } catch (IOException e) {
            }
        }
        return list;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: evaluate(String str)";
    }
}
