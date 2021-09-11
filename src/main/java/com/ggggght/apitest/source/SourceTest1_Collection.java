package com.ggggght.apitest.source;

import com.ggggght.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class SourceTest1_Collection {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		long now = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"));
		List<SensorReading> list = List.of(
				new SensorReading("Sen_1", now, 35.5D),
				new SensorReading("Sen_2", now, 37.8d),
				new SensorReading("Sen_4", now, 35.2D),
				new SensorReading("Sen_6", now, 34.1d)
		);
		DataStream<SensorReading> source = env.fromCollection(list);

		source.print("source");

		DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5, 6);
		integerDataStream.print("int");

		env.execute("...");

	}
}
