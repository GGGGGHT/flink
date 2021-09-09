package com.ggggght.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {
	public static void main(String[] args) throws Exception {
		// 创建流处理执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度
		// env.setParallelism(28);
		// env.setMaxParallelism(28);
		DataStream<String> inputDataStream = env.readTextFile("/Users/admin/github/flink/src/main/resources/hello.txt");

		DataStream<Tuple2<String, Integer>> result = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				Arrays.stream(value.split(" ")).forEach(s -> out.collect(new Tuple2<>(s, 1)));
			}
		}).keyBy(0).sum(1);

		result.print();

		env.execute();
	}
}
