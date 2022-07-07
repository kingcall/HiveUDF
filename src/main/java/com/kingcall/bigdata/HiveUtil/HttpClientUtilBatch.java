package com.kingcall.bigdata.HiveUtil;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
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

public class HttpClientUtilBatch {
    private static PoolingHttpClientConnectionManager CM;
    private static RequestConfig requestConfig;
    private static SocketConfig socketConfig;
    private static Logger logger = LoggerFactory.getLogger(HttpClientUtilBatch.class);
    private static HttpClientBuilder builder;
    private static CloseableHttpClient closeableHttpClient;
    private static String ip;

    static {
        // 设置连接池
        CM = new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory()).build());
        // 设置连接池大小
        CM.setMaxTotal(5);
        // 设置路由的链接数大小，否则链接池不生效 默认是2
        CM.setDefaultMaxPerRoute(5);
        //连接1分钟超时，等待结果3分钟超时
        requestConfig = RequestConfig.custom()
                .setSocketTimeout(60000 * 3)
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
    }

    /**
     * 获取HttpClient实例
     *
     * @return
     */
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
     * @param array
     * @return
     */
    public static JSONArray doPost(String url, JSONArray array) {
        CloseableHttpClient httpClient = getHttpClient();
        StringEntity entity = new StringEntity(array.toJSONString(), ContentType.create(ContentType.APPLICATION_JSON.getMimeType(), "utf-8"));
        HttpPost post = new HttpPost(url);
        post.setEntity(entity);
        JSONArray result = null;
        CloseableHttpResponse response = null;
        try {
            //发送请求
            response = httpClient.execute(post);
            if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                //请求成功
                String res = IOUtils.toString(response.getEntity().getContent(), "utf-8");
                try {
                    result = JSONArray.parseArray(res);
                } catch (Exception e1) {
                    logger.error(String.format("result parseArray error %s", e1.getMessage()));
                    JSONObject jsonObject = JSONObject.parseObject(res);
                    result = new JSONArray();
                    for (int i = 0; i < array.size(); i++) {
                        JSONObject obj = JSONObject.parseObject("{\"success\":false,\"msg\":请求已发出，接口报错：\"" + jsonObject.get("msg") + "\",\"data\":{\"extractedInfo\":[]}}");
                        obj.put("dataId", array.getJSONObject(i).get("dataId"));
                        result.add(obj);
                    }
                }
            } else {
                result = new JSONArray();
                for (int i = 0; i < array.size(); i++) {
                    JSONObject obj = JSONObject.parseObject("{\"success\":false,\"msg\":\"请求已发出，状态码：" + response.getStatusLine().getStatusCode() + "\",\"data\":{\"extractedInfo\":[]}}");
                    obj.put("dataId", array.getJSONObject(i).get("dataId"));
                    result.add(obj);
                }
            }
        } catch (Exception e) {
            logger.error(String.format("do post error %s", e.getMessage()));
            result = new JSONArray();
            for (int i = 0; i < array.size(); i++) {
                JSONObject obj = JSONObject.parseObject("{\"success\":false,\"data\":{\"extractedInfo\":[]}}");
                obj.put("dataId", array.getJSONObject(i).get("dataId"));
                obj.put("msg", e.getMessage());
                result.add(obj);
            }
        } finally {
            if (response != null) {
                if (response.getEntity() != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
            }
        }
        return result;
    }

    public static void main(String[] args) {
        JSONArray jsonArray = new JSONArray();
        JSONObject.parseObject("{\"success\":false,\"msg\":\"请求未发出，异常：\",\"data\":{\"extractedInfo\":[]}}");
        for (int i = 0; i < 10; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("populationGroup", "");
            jsonObject.put("domainListJson", "[\"VRT\",\"全旅程客户\",\"全领域业务\",\"商品化属性\",\"销售线索\"]");
            jsonObject.put("textType", "2");
            jsonObject.put("text", "是");
            jsonObject.put("position", "0");
            jsonObject.put("title", "您是否下载使用过\"长安汽车\"APP");
            jsonObject.put("dataSource", "长安汽车直评");
            jsonObject.put("productName", "");
            jsonObject.put("businessType", "售后问题");
            jsonObject.put("businessScene", "保养服务");
            jsonObject.put("optionScore", "");
            jsonObject.put("dataId", i + "");
        }
        System.out.println(doPost("http://10.17.157.90:8100/changan/voc/analyzer", jsonArray));
    }
}