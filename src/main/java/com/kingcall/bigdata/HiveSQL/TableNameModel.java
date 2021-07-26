package com.kingcall.bigdata.HiveSQL;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class TableNameModel {
    public String dbName;
    public String tableName;

    public static String dealNameMark(String name) {
        if(name.startsWith("`") && name.endsWith("`")) {
            return name.substring(1, name.length()-1);
        }else{
            return name;
        }
    }

    public static TableNameModel parseTableName(String tableName) {
        TableNameModel tableNameModel = new TableNameModel();
        String[] splitTable = tableName.split("\\.");
        if(splitTable.length == 2) {
            tableNameModel.setDbName(splitTable[0]);
            tableNameModel.setTableName(splitTable[1]);
        }else if(splitTable.length == 1) {
            tableNameModel.setTableName(splitTable[0]);
        }
        return tableNameModel;
    }
}
