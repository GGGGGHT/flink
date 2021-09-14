package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("all")
public class TransformTest3_RichFunction {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStreamSource<String> source =
        env.readTextFile("D:\\source\\flink\\src\\main\\resources\\sensor.txt");

    final SingleOutputStreamOperator<SensorReading> dataStream =
        source.map((MapFunction<String, SensorReading>) value -> {
          final String[] split = value.split(",");
          return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });
    DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

    resultStream.print();
    env.execute();
  }
}
