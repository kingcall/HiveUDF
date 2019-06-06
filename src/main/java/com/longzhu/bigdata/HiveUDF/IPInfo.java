package com.longzhu.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * 
 * ip_info
 *
 */
@Description(name = "ip_info",
        value = "_FUNC_(ip[, code]) - return geo and operator info of the IP.",
        extended = "code = country/province/city/isp\n"
            + "return country,province,city,isp if code is not set")

public class IPInfo extends UDF {
    private final transient Text result = new Text();
    private static IPLocator l;
    private static final String fileName = "/user/hdfs/jars/ip.dat"; 
    static {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(new Path(fileName));
            l = IPLocator.loadFromStream(in);
        } catch (Exception e) {
            System.out.println("out: Error Message: " + fileName + " exc: " + e.getMessage());
        }
    }

    public Text evaluate(Text IP, Text code) {
        if (IP == null || code == null)
            return null;
        String r = code.toString();
        LocationInfo res = l.find(IP.toString());
        if (res == null) return null;
        if (r.equals("country")) {
            result.set(res.country);
        } else if (r.equals("province")) {
            result.set(res.state);
        } else if (r.equals("city")) {
            result.set(res.city);
        } else if (r.equals("isp")) {
            result.set(res.isp);
        }
        return result;
    }
    
    public Text evaluate(Text IP) {
        LocationInfo res = l.find(IP.toString());
        if (res == null) return null;
        result.set(res.toString());
        return result;
    }
}
