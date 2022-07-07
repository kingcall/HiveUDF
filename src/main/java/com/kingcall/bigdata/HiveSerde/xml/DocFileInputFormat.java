package com.kingcall.bigdata.HiveSerde.xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class DocFileInputFormat extends TextInputFormat implements JobConfigurable {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new DocRecordReader(job, (FileSplit) split);
    }
}
