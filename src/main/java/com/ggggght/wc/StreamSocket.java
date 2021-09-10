package com.ggggght.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamSocket {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String host = parameterTool.get("host");
		int port = parameterTool.getInt("port");
		DataStream<String> socketTextStream = env.socketTextStream(host, port);
		socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				Arrays.stream(value.split(" ")).forEach(s -> out.collect(new Tuple2<>(s, 1)));
			}
		}).keyBy(0).sum(1).print();

		env.execute();
	}
}
