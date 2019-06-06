package com.longzhu.bigdata.HiveUDF;

import java.util.ArrayList;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.library.DicLibrary;
import org.ansj.library.StopLibrary;
import org.ansj.util.MyStaticValue;

/**
 * Chinese words segmentation with user-dict in longzhu
 * use Ansj(a java open source analyzer)
 */

@Description(name = "ansj_seg", value = "_FUNC_(str) - chinese words segment using ansj. Return list of words.",
        extended = "Example: select _FUNC_('我是测试字符串') from src limit 1;\n"
                + "[\"我\", \"是\", \"测试\", \"字符串\"]")

public class AnsjSeg extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private static final String userDic = "/user/hdfs/jars/ansj_dic/longzhu.dic";

    //load userDic in hdfs
    static {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(new Path(userDic));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line = null;
            String[] strs = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    strs = line.split("\t");
                    strs[0] = strs[0].toLowerCase();
                    DicLibrary.insert(DicLibrary.DEFAULT, strs[0]); //ignore nature and freq
                }
            }
            MyStaticValue.isNameRecognition = Boolean.FALSE;
            MyStaticValue.isQuantifierRecognition = Boolean.TRUE;
        } catch (Exception e) {
            System.out.println("Error when load userDic" + e.getMessage());
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1 || arguments.length > 2) {
            throw new UDFArgumentLengthException(
                    "The function AnsjSeg(str) takes 1 or 2 arguments.");
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
        ArrayList<Text> result = new ArrayList<>();

        if (filterStop) {
            for (Term words : DicAnalysis.parse(s.toString()).recognition(StopLibrary.get())) {
                if (words.getName().trim().length() > 0) {
                    result.add(new Text(words.getName().trim()));
                }
            }
        } else {
            for (Term words : DicAnalysis.parse(s.toString())) {
                if (words.getName().trim().length() > 0) {
                    result.add(new Text(words.getName().trim()));
                }
            }
        }
        return result;
    }


    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("ansj_seg", children);
    }
}
