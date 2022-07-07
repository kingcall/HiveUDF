package com.kingcall.bigdata.HiveSerde.xml;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class DocRecordWriter implements RecordWriter {

    private FSDataOutputStream out;
    private final String DOC_START = "<DOC>";
    private final String DOC_END = "</DOC>";

    public DocRecordWriter(FSDataOutputStream o) {
        this.out = o;
    }

    @Override
    public void close(boolean abort) throws IOException {
        out.flush();
        out.close();
    }


    @Override
    public void write(Writable wr) throws IOException {
        write(DOC_START);
        write("\n");
        write(wr.toString());
        write("\n");
        write(DOC_END);
        write("\n");
    }



    private void write(String str) throws IOException {
        out.write(str.getBytes(), 0, str.length());
    }

}