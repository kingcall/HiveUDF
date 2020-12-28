package com.kingcall.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * CheckTimeSlice
 *
 */
@Description(name = "check_time_slice",
        value = "_FUNC_(val, slices) - return info of the right slice in slices str.",
        extended = "slices format: info1,start1,end1|info2,start2,end2...\n"
            + "return info_x if val between start_x and end_x")

public class CheckTimeSlice extends UDF {
    private final transient Text result = new Text();
    public Text evaluate(LongWritable val, Text slices) {
        if (slices == null || val == null)
            return null;
        String[] intervals = slices.toString().split("\\|");
        long value = val.get();
        for (int i = 0; i < intervals.length; ++i) {
            String[] startEnd = intervals[i].split(",");
            if (startEnd.length != 3)
                return null;
            if (value >= Long.parseLong(startEnd[1]) && value <= Long.parseLong(startEnd[2])) {
                result.set(startEnd[0]);
                return result;
            }
        }
        return null;
    }
}
