package com.kingcall.bigdata.HiveUDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;

@Description(name = "ComplexEventProcessUDAF", value = "_FUNC_(x) - Returns the cpunt of a set of match patterns")
public class ComplexEventProcessUDAF extends AbstractGenericUDAFResolver {
}
