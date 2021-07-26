package com.kingcall.bigdata.HiveSQL;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

// 字段名
public class FieldNameModel {
    private String dbName;
    private String tableName;
    private String fieldName;
}
// 带过程的表字段

class FieldNameWithProcessModel {
    private String dbName;
    private String tableName;
    private String fieldName;
    private String process;
}

// 解析单个select中存储字段和别名
// 如：select a+b as c from table;
// 存储数据为 fieldNames:[a,b] alias:c process:a+b
class HiveFieldLineageSelectItemModel {
    private Set<String> fieldNames;
    private String alias;
    private String process;
}
// 解析单个select后的结果

class HiveFieldLineageSelectModel {
    Integer id; // index
    Integer parentId; // 父id，第一层select为null
    TableNameModel fromTable; // 来源表，来源子select则为null
    String tableAlias; // 表别名
    List<HiveFieldLineageSelectItemModel> selectItems; // select字段
}
// 血缘结果结构

class HiveFieldLineageModel {
    private FieldNameModel targetField; // 目标字段
    private HashSet<FieldNameModel> sourceFields; // 来源字段列表
}

