package com.kingcall.bigdata.HiveUtil;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils;

import java.net.SocketTimeoutException;

public class
HttpClientUtil {
    private final static HttpConnectionManager httpConnectionManager;

    static {
        httpConnectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = httpConnectionManager.getParams();
        params.setConnectionTimeout(8000);
        params.setSoTimeout(8000);
        params.setDefaultMaxConnectionsPerHost(2000);
        params.setMaxTotalConnections(2000);
    }

    /**
     * post请求
     *
     * @param url
     * @param json
     * @return
     */
    public static String doPost(String url, JSONObject json) {
        HttpClient httpClient = new HttpClient(httpConnectionManager);
        PostMethod postMethod = new PostMethod(url);
        postMethod.addRequestHeader("accept", "application/json");
        postMethod.addRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");

        String res = "";

        json.entrySet().forEach(entry -> {
            postMethod.addParameter(entry.getKey(), entry.getValue().toString());
        });
        long start = System.currentTimeMillis();
        try {
            int code = httpClient.executeMethod(postMethod);
            if (code==200){
                res = IOUtils.toString(postMethod.getResponseBodyAsStream(), "utf-8");
            }else{
                System.out.println(String.format("request error:%s", IOUtils.toString(postMethod.getResponseBodyAsStream(), "utf-8")));
            }
        }
        catch (SocketTimeoutException| ConnectTimeoutException e){
            System.out.println(String.format("request timeout %s", json.toJSONString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        } finally {
            postMethod.releaseConnection();
        }
        JSONObject jsb = JSON.parseObject(res);
        jsb.put("time", System.currentTimeMillis() - start);
        return jsb.toJSONString();
    }


    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("index", "我是测试字符串");
        jsonObject.put("startDate", "我是测试字符串");
        jsonObject.put("endDate", "我是测试字符串");
        System.out.println(doPost("http://localhost:8080/data/notice", jsonObject));
    }
}