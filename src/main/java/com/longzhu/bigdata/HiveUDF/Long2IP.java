package com.longzhu.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * Long2IP
 *
 */
@Description(name = "long2ip",
        value = "_FUNC_(ip[, inv]) - return dot string type of ip",
        extended = "ip: integer type of ip, inv: 0-normal(default) 1-inverse for net endian")

public class Long2IP extends UDF {
    private final transient Text result = new Text();
    public Text evaluate(LongWritable ip) {
        if(ip == null) return null;

        long ip_num = ip.get();
        long[] tmp = new long[4];
        for(int i = 0; i < 4; ++i) {
            tmp[i] = ip_num & 0xff;
            ip_num = ip_num >> 8;
        }

        String res = String.format("%d.%d.%d.%d", tmp[3], tmp[2], tmp[1], tmp[0]);
        result.set(res);
        return result;
    }

    public Text evaluate(LongWritable ip, IntWritable inv) {
        if(ip == null || inv == null) return null;

        long ip_num = ip.get();
        long inv_num = inv.get();
        if(inv_num < 0 || inv_num > 1) return null;

        long[] tmp = new long[4];
        for(int i = 0; i < 4; ++i) {
            tmp[i] = ip_num & 0xff;
            ip_num = ip_num >> 8;
        }

        String res = "";
        if (inv.get() == 0) {
            res = String.format("%d.%d.%d.%d", tmp[3], tmp[2], tmp[1], tmp[0]);
        } else {
            res = String.format("%d.%d.%d.%d", tmp[0], tmp[1], tmp[2], tmp[3]);
        }
        result.set(res);
        return result;
    }
}
