package com.kingcall.bigdata.HiveUDF;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Hive2Kafka extends GenericUDF {
    ConstantObjectInspector hostAndPortListStringInspector;
    ConstantObjectInspector topicInspector;
    ListObjectInspector dataListInspector;
    MapObjectInspector dataElementInspector;

    String hostAndPort;
    String topics;

    KafkaProducer kafkaProducer;
    private static Logger logger= LoggerFactory.getLogger(Hive2Kafka.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        /**
         * 验证参数个数 因为是通用接口，所以参数个数是固定的
         */
        if (objectInspectors.length != 4) {
            throw new UDFArgumentException("please input three parameter " +
                    "1 host:port list" +
                    "2 topic_name" +
                    "3 data_mark" +
                    "4  data ARRAY<MAP<STRING<STRING>>"
            );
        }
        // 验证参数类型 第一个参数 kafka broker 和端口
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE) ||
                ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ) {
            throw new UDFArgumentException("broker host:port  must be constant");
        }
        // 验证参数类型 验证第二个参数 topic
        if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE) ||
                ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
        ) {
            throw new UDFArgumentException("kafka topic must be constant");
        }
        // 验证参数类型 验证第3四个参数 数据
        if (!objectInspectors[2].getCategory().equals(ObjectInspector.Category.LIST)) {
            throw new UDFArgumentException(" Expecting an array field as third argument");
        }
        if (((ListObjectInspector) objectInspectors[2]).getListElementObjectInspector().getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException(" Expecting an array<map<string,string>> field as third argument");
        }
        // 获取参数的 Inspector
        hostAndPortListStringInspector = (ConstantObjectInspector) objectInspectors[0];
        topicInspector = (ConstantObjectInspector) objectInspectors[1];
        dataListInspector = (ListObjectInspector) objectInspectors[2];
        dataElementInspector = (MapObjectInspector) dataListInspector.getListElementObjectInspector();

        hostAndPort = hostAndPortListStringInspector.getWritableConstantValue().toString();
        topics = topicInspector.getWritableConstantValue().toString();

        // 定义返回数据的 发送数据成功我们就返回success 否则就返回false
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        int nelements = dataListInspector.getListLength(arguments[3].get());
        final String[] resultMessage = {"成功"};
        for (int i = 0; i < nelements; i++) {
            // 一行记录
            Object row = dataListInspector.getListElement(arguments[2].get(), i);
            Map<?, ?> map =dataElementInspector.getMap(row);
            // 转成标准的java map，否则里面的key value字段为hadoop writable对象
            Map<String, String> data = new HashMap<String,String>();
            map.forEach((k,v)->{
                data.put(k.toString(), v.toString());
            });

            String jsonStr = JSON.toJSONString(map);
            // 这里我们就不指定分区策略了，采取默认的轮训，当然你可以可以指定消息的key  key= data.get("key");
            // 甚至你这里可以采取不同的发送方式 或者是开启kafka 事物或者幂等又或者是采取异步的发送
            kafkaProducer.send(new ProducerRecord<String, String>(topics, jsonStr), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e!=null){
                        resultMessage[0] = "失败";
                        logger.error(String.format("send fail:%s", jsonStr));
                    }
                }
            });

        }
        // 返回每一批数据的发送情况
        // 其实如果我们也可以把每一条发送失败的消息返回回去，或者是把 每一条发送失败的消息的id 返回回去
        return resultMessage[0];
    }

    private void initKafkaClient() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", hostAndPort);
        kafkaProps.put("acks", "all");
        // 重试三次
        kafkaProps.put("retries", 3);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 开启幂等
        kafkaProps.put("enable.idempotence", "true");
        kafkaProducer = new KafkaProducer<String, String>(kafkaProps);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "hive2kafka(brokerhost_and_port,topic, array<map<string,string>>)";
    }
}
