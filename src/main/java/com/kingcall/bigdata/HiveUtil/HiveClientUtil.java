package com.kingcall.bigdata.HiveUtil;

import groovy.util.logging.Slf4j;
import lombok.Setter;
import org.json.JSONArray;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

// 数据
@Setter
class HiveSqlResultModel {
    // 表头
    List<JSONArray> meta;

    // 数据
    List<JSONArray> data;

    //长度
    Integer count;
}

// 表头
@Setter
class HiveSqlResultColumnModel {
    // 字段名
    String name;

    // 类型
    String type;

    // 精度
    Integer precision;

    // 小数位数
    Integer scale;


}
@Slf4j
public class HiveClientUtil {

    /**
     * 驱动
     * 默认队列
     * jdbc地址
     * 用户名
     * 密码
     * 前置sql
     */
    private String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";
    private String QUEUE = "default";
    private String hiveurl;
    private String username;
    private String password;
    private List<String> defaultPreSql;

    /**
     * 构造的时候初始化上面的配置
     */
    public HiveClientUtil(String host, String port, String username, String password, String database) {
        this.hiveurl = String.format("jdbc:hive2://%s:%s/%s", host, port, database);
        this.username = username;
        this.password = password;
        this.defaultPreSql = new ArrayList<>();
        this.defaultPreSql.add(String.format("SET tez.queue.name=%s", QUEUE));
    }

    /**
     * 执行前置sql
     */
    private void execPreSql(PreparedStatement preparedStatement, List<String> presqls) {
        presqls.forEach(presql -> {
            try {
                preparedStatement.execute(presql);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("前置SQL执行失败");
            }
        });
    }

    /**
     * 运行sql
     */
    public HiveSqlResultModel execute(String sql) throws SQLException {
        return this.execute(sql, null);
    }

    /**
     * 运行前置sql和主sql
     */
    public HiveSqlResultModel execute(String sql, List<String> presqls) throws SQLException {
        try {
            Class.forName(DRIVERNAME);
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("sql执行初始化失败");
        }
        // 连接
        Connection conn = DriverManager.getConnection(hiveurl, username, password);

        // 主sql加载
        PreparedStatement preparedStatement = conn.prepareStatement(sql);

        // 执行前置配置sql
        execPreSql(preparedStatement, defaultPreSql);
        if(presqls != null) {
            execPreSql(preparedStatement, presqls);
        }

        // 获取执行结果，表头，列数量
        ResultSet result = preparedStatement.executeQuery();
        ResultSetMetaData metaData = result.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 获取表头数据并转换
        HiveSqlResultModel hiveSqlResultModel = new HiveSqlResultModel();
        List<JSONArray> metas = new ArrayList<>();
        for(int i = 1; i <= columnCount; i++) {
            HiveSqlResultColumnModel columnModel = new HiveSqlResultColumnModel();
            columnModel.setName(metaData.getColumnName(i));
            columnModel.setType(metaData.getColumnTypeName(i));
            if("DECIMAL".equals(metaData.getColumnTypeName(i))){
                columnModel.setPrecision(metaData.getPrecision(i));
                columnModel.setScale(metaData.getScale(i));
            }

            JSONArray array = new JSONArray();
            array.put(columnModel);
            metas.add(array);
        }
        hiveSqlResultModel.setMeta(metas);

        // 处理每行数据，并计数
        List<JSONArray> data = new ArrayList<>();
        int count = 0;
        while(result.next()) {
            JSONArray rowData = new JSONArray();
            for(int i = 1; i <= columnCount; i++) {
                rowData.put(result.getString(i));
            }
            data.add(rowData);
            count++;
        }
        hiveSqlResultModel.setCount(count);
        hiveSqlResultModel.setData(data);
        return hiveSqlResultModel;
    }
}
