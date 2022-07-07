package com.kingcall.bigdata.HiveUDF.demo;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class UdfWithMuiltiArg extends UDF {
    public String evaluate(String a,String b){
        if (a.contains(b)){
            return "a";
        }else {
            return "b";
        }
    }

    public Text evaluate(Text input) {
        return new Text("Hello " + input.toString());
    }

    @Test
    public void testUDF() {
        UdfWithMuiltiArg example = new UdfWithMuiltiArg();
        Assert.assertEquals("Hello world", example.evaluate(new Text("world")).toString());
    }

}
