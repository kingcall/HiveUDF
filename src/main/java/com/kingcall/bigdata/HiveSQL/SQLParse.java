package com.kingcall.bigdata.HiveSQL;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import static org.apache.calcite.sql.advise.SqlAdvisor.LOGGER;

@Slf4j
public class SQLParse {
    ParseDriver pd = new ParseDriver();

    public static void main(String[] args) throws Exception {
        SQLParse sqlParse = new SQLParse();
        sqlParse.parse("select * from ods.text");

    }


    /**
     * sql解析
     *
     * @param sql
     */
    public void parse(String sql) {
        try {
            ASTNode tree = pd.parse(sql);
            while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
                tree = (ASTNode) tree.getChild(0);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
