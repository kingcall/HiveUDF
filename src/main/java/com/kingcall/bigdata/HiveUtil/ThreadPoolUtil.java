package com.kingcall.bigdata.HiveUtil;

import com.alibaba.fastjson.JSONObject;

public class ThreadPoolUtil {
    public static void main(String[] args) {
        String errMsg = "pos 24, line 1, column 25{\"success\":false,\"msg\":???????????\"Handler dispatch failed; nested exception is java.lang.OutOfMemoryError: unable to create new native thread\",\"data\":{\"extractedInfo\":[]}}\n";

        JSONObject obj = JSONObject.parseObject("{\"success\":false,\"data\":{\"extractedInfo\":[]}}");
        obj.put("msg", errMsg);
        System.out.println(obj.toJSONString());

    }

}
