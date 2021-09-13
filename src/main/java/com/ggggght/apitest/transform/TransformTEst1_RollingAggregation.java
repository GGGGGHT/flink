package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("all")
public class TransformTEst1_RollingAggregation {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStreamSource<String> source =
        env.readTextFile("D:\\source\\flink\\src\\main\\resources\\sensor.txt");

    source.map((MapFunction<String, SensorReading>) value -> {
      final String[] split = value.split(",");
      return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
    }).keyBy(SensorReading::getId).reduce((value1, value2) -> new SensorReading() {{
      setId(value2.getId());
      setTemperature(Math.max(value1.getTemperature(), value2.getTemperature()));
      setTimestamp(value2.getTimestamp());
    }}).print();
    //keydStream.maxBy("temperature").print("max temperature");
    env.execute();
  }
}
