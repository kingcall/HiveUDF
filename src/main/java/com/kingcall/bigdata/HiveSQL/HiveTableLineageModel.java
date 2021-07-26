package com.kingcall.bigdata.HiveSQL;


import java.util.HashSet;

// 表血缘结构，对单个sql
public class HiveTableLineageModel {
    // 输出的表名
    private TableNameModel outputTable;
    // 输入的表名列表
    private HashSet<TableNameModel> inputTables;
}
