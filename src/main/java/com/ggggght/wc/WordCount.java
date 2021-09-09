package com.ggggght.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * word count
 * flatMap中的参数不可以使用lambda表达式
 * 针对离线数据集进行批处理
 */
public class WordCount {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> inputData = env.readTextFile("/Users/admin/github/flink/src/main/resources/hello.txt");
		DataSet<Tuple2<String, Integer>> result = inputData.flatMap(
				new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						Arrays.stream(value.split(" ")).forEach(s -> out.collect(new Tuple2<>(s, 1)));
					}
				}
		).groupBy(0).sum(1);
		result.print();
	}
}
