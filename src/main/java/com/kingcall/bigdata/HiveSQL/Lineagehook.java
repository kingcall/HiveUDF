package com.kingcall.bigdata.HiveSQL;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @description: SQL 解析
 * ExecuteWithHookContext的实现比直接用antlr4解析方便很多，代码量也比较少。
 * hivehook解析完数据后，可以通过消息发送到MQ中，后续后端进行采集消费，
 */
public class Lineagehook implements ExecuteWithHookContext {

    private Logger logger = LoggerFactory.getLogger(Lineagehook.class);

    // 输出表
    private Set<String> inputTables;

    // 输入表
    private Set<String> outputTables;

    // 字段血缘 Map
    // key为输出字段，value为来源字段数组
    private Map<String, ArrayList<String>> fieldLineage;

    public Lineagehook() {
        inputTables = new HashSet<>();
        outputTables = new HashSet<>();
        fieldLineage = new HashMap<>();
    }

    // 处理表的格式为 库.表
    private String dealOutputTable(Table table) {
        String dbName = table.getDbName();
        String tableName = table.getTableName();
        return dbName != null ? String.format("%s.%s", dbName, tableName) : tableName;
    }

    // 处理输出字段的格式
    private String dealDepOutputField(LineageInfo.DependencyKey dependencyKey) {
        try {
            String tableName = dealOutputTable(dependencyKey.getDataContainer().getTable());
            String field = dependencyKey.getFieldSchema().getName();
            return String.format("%s.%s", tableName, field);
        } catch (Exception e) {
            logger.error("deal dep output field error" + e.getMessage());
            return null;
        }
    }

    // 处理来源字段的格式
    private String dealBaseOutputField(LineageInfo.BaseColumnInfo baseColumnInfo) {
        try {
            String tableName = dealOutputTable(baseColumnInfo.getTabAlias().getTable());
            String field = baseColumnInfo.getColumn().getName();
            return String.format("%s.%s", tableName, field);
        } catch (Exception e) {
            logger.error("deal base output field error" + e.getMessage());
            return null;
        }
    }

    // 主要重写的方法，入口，
    // 血缘的信息在hookContext.getLinfo()
    // 结构是集合，每个是一个map，代表一个字段的血缘，
    // 每个map的key为输出字段，value为来源字段
    // 处理表血缘就直接忽略字段，因为存在set里就避免重复
    // 处理字段血缘就直接分别处理key value的每个即可，最终也存储在类似的map中
    @Override
    public void run(HookContext hookContext) {
        for (Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> dep : hookContext.getLinfo().entrySet()) {
            // 表血缘
            Optional.ofNullable(dep.getKey())
                    .map(LineageInfo.DependencyKey::getDataContainer)
                    .map(LineageInfo.DataContainer::getTable)
                    .map(this::dealOutputTable)
                    .ifPresent(outputTables::add);
            Optional.ofNullable(dep.getValue())
                    .map(LineageInfo.Dependency::getBaseCols)
                    .ifPresent(items -> items.stream().map(LineageInfo.BaseColumnInfo::getTabAlias)
                            .map(LineageInfo.TableAliasInfo::getTable)
                            .map(this::dealOutputTable)
                            .forEach(inputTables::add));

            // 字段血缘
            String column = Optional.ofNullable(dep.getKey())
                    .map(this::dealDepOutputField)
                    .map(aimField -> {
                        fieldLineage.put(aimField, new ArrayList<>());
                        return aimField;
                    }).orElse(null);
            Optional.ofNullable(dep.getValue())
                    .map(LineageInfo.Dependency::getBaseCols)
                    .ifPresent(items -> items.stream()
                            .map(this::dealBaseOutputField)
                            .forEach(item -> {
                                fieldLineage.get(column).add(item);
                            }));
        }
        System.out.println("来源表:");
        System.out.println(inputTables);
        System.out.println("输出表:");
        System.out.println(outputTables);
        System.out.println("字段血缘:");
        System.out.println(fieldLineage.toString());
    }

}
