package com.ggggght.apitest.source;

import com.ggggght.bean.SensorReading;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class SourceTest2_Kafka {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "consumer-group");
		//properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//properties.setProperty("auto.offset.reset", "latest");

		DataStream<String> dataSource = env.addSource(
				new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

		dataSource.print();
		env.execute();
	}
}
