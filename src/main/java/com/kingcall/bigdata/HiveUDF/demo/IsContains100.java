package com.kingcall.bigdata.HiveUDF.demo;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IsContains100 extends UDF{

    public String evaluate(String s){

        if(s == null || s.length() == 0){
            return "0";
        }

        return s.contains("100") ? "1" : "0";
    }
}
