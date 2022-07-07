package com.kingcall.bigdata.HiveUtil;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class HttpClientUtilThread {
    private static PoolingHttpClientConnectionManager CM;
    private static RequestConfig requestConfig;
    private static SocketConfig socketConfig;
    private static Logger logger = LoggerFactory.getLogger(HttpClientUtilThread.class);
    private static HttpClientBuilder builder;
    private static CloseableHttpClient closeableHttpClient;
    private static String ip;

    static {
        // 设置连接池
        CM = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory()).build());
        // 设置连接池大小
        CM.setMaxTotal(10);
        // 设置路由的链接数大小，否则链接池不生效 默认是2
       CM.setDefaultMaxPerRoute(10);
        //官方推荐使用这个来检查永久链接的可用性，而不推荐每次请求的时候才去检查（ms）
       //CM.setValidateAfterInactivity(5000);

        requestConfig = RequestConfig.custom()
                .setSocketTimeout(60000)
                .setConnectTimeout(60000)
                .build();
        SocketConfig socketConfig = SocketConfig.custom().setSoKeepAlive(true).build();
        //DefaultHttpRequestRetryHandler不传任何参数, 默认是重试3次
        HttpRequestRetryHandler requestRetryHandler = new DefaultHttpRequestRetryHandler(5, false);
        builder = HttpClientBuilder.create();
        builder.setConnectionManager(CM);
        builder.setDefaultRequestConfig(requestConfig);
        builder.setDefaultSocketConfig(socketConfig);
        builder.setRetryHandler(requestRetryHandler);
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HttpClient实例
     *
     * @return
     */
//    public static synchronized CloseableHttpClient getHttpClient() {
//        if (closeableHttpClient == null) {
//            closeableHttpClient =  builder.build();
//        }
//        return closeableHttpClient;
//
//    }

    public static CloseableHttpClient getHttpClient() {
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setConnectionManager(CM);
        builder.setDefaultRequestConfig(requestConfig);
        builder.setDefaultSocketConfig(socketConfig);
        return builder.build();
    }

    /**
     * post请求
     *
     * @param url
     * @param json
     * @return
     */
    public static String doPost(String url, JSONObject json) {
        long start = System.currentTimeMillis();
        CloseableHttpClient httpClient = getHttpClient();
        //logger.info(String.format("request info: client time:%d %s", System.currentTimeMillis() - start, CM.getTotalStats().toString()));
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, Object>> iterator = json.entrySet().iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (i > 0) {
                sb.append("&");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            i++;
        }
        StringEntity entity = new StringEntity(sb.toString(), ContentType.create(ContentType.APPLICATION_FORM_URLENCODED.getMimeType(), "utf-8"));
        HttpPost post = new HttpPost(url);
        post.setEntity(entity);
        String res = "{\"success\":false,\"msg\":\"非接口返回\",\"data\":{\"extractedInfo\":[]}}";
        JSONObject jsb = JSON.parseObject(res);
        start = System.currentTimeMillis();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(post);
            if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                res = IOUtils.toString(response.getEntity().getContent(), "utf-8");
                jsb = JSON.parseObject(res);
                jsb.put("msg", response.getStatusLine().getStatusCode());
            } else {
                logger.warn(String.format("request error:%s", IOUtils.toString(response.getEntity().getContent(), "utf-8")));
                if (response==null){
                    jsb.put("msg", errorMessage("response is null:"));
                }else {
                    jsb.put("msg", errorMessage(String.valueOf(response.getStatusLine().getStatusCode())));
                }
            }

        }
        catch(ConnectionPoolTimeoutException e){
            logger.error(String.format("pool timeout %s", e.getMessage()));
            jsb.put("msg", errorMessage(e.getMessage()));
        }
        catch (SocketTimeoutException | ConnectTimeoutException e) {
            logger.error(String.format("request timeout %s", e.getMessage()));
            jsb.put("msg", errorMessage(e.getMessage()));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error(e.getMessage());
            jsb.put("msg", errorMessage(e.getMessage()));
            e.printStackTrace();
        } finally {
            if (response != null) {
                if (response.getEntity() != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
                //response.close(); //否则conn 不能被复用
            }
        }

        jsb.put("time", System.currentTimeMillis() - start);
        return jsb.toJSONString();
    }

    private static String errorMessage(String messsage){
        //return messsage+":"+ip;
        return messsage;
    }


    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("populationGroup", "我是测试字符串");
        jsonObject.put("domainListJson", "[\"VRT\",\"汽车\"]");
        jsonObject.put("textType", "1");
        jsonObject.put("text", "360全景");
        jsonObject.put("position", "0");
        jsonObject.put("title", "我是测试字符串");
        jsonObject.put("dataSource", "联络中心热线服务");
        jsonObject.put("productName", "我是测试字符串");
        System.out.println(doPost("http://10.17.157.90:8100/changan/voc/analyzer", jsonObject));
    }
}