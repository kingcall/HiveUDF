package com.kingcall.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.udf.UDFBaseBitOP;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Count 1 bit in value
 *
 */
@Description(name = "count_one_bit", value = "_FUNC_(value) - Count 1 bits in value", extended = "Returns number of 1-bit in value(tinyint/smallint/int/bigint"
        + "\nExample:\n  > SELECT _FUNC_(37);\n  3")
public class BitCountOne extends UDFBaseBitOP {

    public BitCountOne() {
    }

    public IntWritable evaluate(ByteWritable value) {
        if (value == null) {
            return null;
        }
        int cnt = 0;
        int v = value.get();
        for (cnt = 0; v != 0; cnt++) {
            v &= (v - 1);
        }
        intWritable.set(cnt);
        return intWritable;
    }

    public IntWritable evaluate(ShortWritable value) {
        if (value == null) {
            return null;
        }
        int cnt = 0;
        int v = value.get();
        for (cnt = 0; v != 0; cnt++) {
            v &= (v - 1);
        }
        intWritable.set(cnt);
        return intWritable;
    }

    public IntWritable evaluate(IntWritable value) {
        if (value == null) {
            return null;
        }
        int cnt = 0;
        int v = value.get();
        for (cnt = 0; v != 0; cnt++) {
            v &= (v - 1);
        }
        intWritable.set(cnt);
        return intWritable;
    }

    public IntWritable evaluate(LongWritable value) {
        if (value == null) {
            return null;
        }
        int cnt = 0;
        long v = value.get();
        for (cnt = 0; v != 0; cnt++) {
            v &= (v - 1);
        }
        intWritable.set(cnt);
        return intWritable;
    }
}
