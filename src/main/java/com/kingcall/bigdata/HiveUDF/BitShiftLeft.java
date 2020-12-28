package com.kingcall.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.udf.UDFBaseBitOP;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Copy From org.apache.hadoop.hive.ql.udf.UDFOPBitLeftShift.
 *
 */
@Description(name = "shiftleft", value = "_FUNC_(a, b) - Bitwise left shift", extended = "Returns int for tinyint, smallint and int a. Returns bigint for bigint a."
        + "\nExample:\n  > SELECT _FUNC_(2, 1);\n  4")
public class BitShiftLeft extends UDFBaseBitOP {

    public BitShiftLeft() {
    }

    public IntWritable evaluate(ByteWritable a, IntWritable b) {
        if (a == null || b == null) {
            return null;
        }
        intWritable.set(a.get() << b.get());
        return intWritable;
    }

    public IntWritable evaluate(ShortWritable a, IntWritable b) {
        if (a == null || b == null) {
            return null;
        }
        intWritable.set(a.get() << b.get());
        return intWritable;
    }

    public IntWritable evaluate(IntWritable a, IntWritable b) {
        if (a == null || b == null) {
            return null;
        }
        intWritable.set(a.get() << b.get());
        return intWritable;
    }

    public LongWritable evaluate(LongWritable a, IntWritable b) {
        if (a == null || b == null) {
            return null;
        }
        longWritable.set(a.get() << b.get());
        return longWritable;
    }
}
