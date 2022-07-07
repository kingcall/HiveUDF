package com.kingcall.bigdata.HiveSerde.xml;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings({ "rawtypes" })
public class DocFileOutputFormat<K extends WritableComparable, V extends Writable>
        extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {

    public RecordWriter getHiveRecordWriter(JobConf job, Path outPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress)
            throws IOException {
        FileSystem fs = outPath.getFileSystem(job);
        FSDataOutputStream out = fs.create(outPath);

        return new DocRecordWriter(out);
    }
}