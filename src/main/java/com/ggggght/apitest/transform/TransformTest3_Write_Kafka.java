package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

@SuppressWarnings("all")
public class TransformTest3_Write_Kafka {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStreamSource<String> source =
        env.readTextFile("D:\\source\\flink\\src\\main\\resources\\sensor.txt");

    final DataStream<String> sensorReadingDataStream = source.map(line -> {
      final String[] split = line.split(",");
      return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])).toString();
    });

    final DataStreamSink<String> sinktest = sensorReadingDataStream.addSink(
        new FlinkKafkaProducer<String>("localhost:9092", "sinktest", new SimpleStringSchema()));
    env.execute();
  }
}
