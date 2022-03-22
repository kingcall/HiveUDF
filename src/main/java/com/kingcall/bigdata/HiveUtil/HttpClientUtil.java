package com.kingcall.bigdata.HiveUtil;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;

import java.io.IOException;

public class HttpClientUtil {


    private static HttpClientParams getParams(){
        HttpClientParams params=new HttpClientParams();
        params.setConnectionManagerTimeout(1000);
        params.setSoTimeout(1000);
        return params;
    }
    /**
     * post请求
     * @param url
     * @param json
     * @return
     */
    public static String doPost(String url, JSONObject json){
        HttpClient httpClient = new HttpClient();
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(300);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(3000);
        PostMethod postMethod = new PostMethod(url);

        postMethod.addRequestHeader("accept", "*/*");
        postMethod.addRequestHeader("connection", "Keep-Alive");
        //设置json格式传送
        postMethod.addRequestHeader("Content-Type", "application/json;charset=GBK");
        //必须设置下面这个Header
        postMethod.addRequestHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36");
        //添加请求参数 通过遍历的方式添加
        json.entrySet().forEach(entry->{
            postMethod.addParameter(entry.getKey(),entry.getValue().toString());
        });


        String res = "";
        try {
            int code = httpClient.executeMethod(postMethod);
            if (code == 200){
                res = postMethod.getResponseBodyAsString();
            }
        } catch (IOException e) {
            e.printStackTrace();
            res = e.getMessage();
        }
        return res;
    }

    public static void main(String[] args) {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("commentId", "13026194071");
        System.out.println(doPost("http://tcc.taobao.com/cc/json/mobile_tel_segment.htm?tel=13026194071", jsonObject));
    }
}