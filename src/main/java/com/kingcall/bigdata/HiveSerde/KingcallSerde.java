package com.kingcall.bigdata.HiveSerde;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @description: 自定义序列化
 * 继承自AbstractSerDe，主要实现下面的initialize，serialize，deserialize
 */
public class KingcallSerde extends AbstractSerDe {

    private static final Logger logger = LoggerFactory.getLogger(KingcallSerde.class);

    // 用于存储字段名
    private List<String> columnNames;

    // 用于存储字段类型
    private List<TypeInfo> columnTypes;
    private ObjectInspector objectInspector;

    // 初始化，在serialize和deserialize前都会执行initialize
    @Override
    public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties) throws SerDeException {
        String columnNameString = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeString = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnNames = Arrays.asList(columnNameString.split(","));
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeString);

        List<ObjectInspector> columnOIs = new ArrayList<>();
        ObjectInspector oi;
        for(int i = 0; i < columnNames.size(); i++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(i));
            columnOIs.add(oi);
        }
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    }

    // 重载的方法，直接调用上面的实现
    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties) throws SerDeException {
        this.initialize(configuration, properties, null);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;
    }

    // o是导入的单行数据的数组，objInspector包含了导入的字段信息，这边直接就按顺序
    // 将数据处理成key=value,key1=value1的格式的字符串，并返回Writable格式。
    @Override
    public Writable serialize(Object o, ObjectInspector objInspector) throws SerDeException {
        Object[] arr = (Object[]) o;
        List<String> tt = new ArrayList<>();
        for (int i = 0; i < arr.length; i++) {
            tt.add(String.format("%s=%s", columnNames.get(i), arr[i].toString()));
        }
        return new Text(StringUtils.join(tt, ","));
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    // writable转为字符串，其中包含了一行的信息，如key=value,key1=value1
    // 分割后存到map中，然后按照字段的顺序，放到object中
    // 中间还需要做类型处理，这边只简单的做了string和int
    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        Text text = (Text) writable;
        Map<String, String> map = new HashMap<>();
        String[] cols = text.toString().split(",");
        for(String col: cols) {
            String[] item = col.split("=");
            map.put(item[0], item[1]);
        }
        ArrayList<Object> row = new ArrayList<>();
        Object obj = null;
        for(int i = 0; i < columnNames.size(); i++){
            TypeInfo typeInfo = columnTypes.get(i);
            PrimitiveTypeInfo pTypeInfo = (PrimitiveTypeInfo)typeInfo;
            if(typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                if(pTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING){
                    obj = StringUtils.defaultString(map.get(columnNames.get(i)));
                }
                if(pTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                    obj = Integer.parseInt(map.get(columnNames.get(i)));
                }
            }
            row.add(obj);
        }
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public String getConfigurationErrors() {
        return super.getConfigurationErrors();
    }

    @Override
    public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
        return super.shouldStoreFieldsInMetastore(tableParams);
    }
}
