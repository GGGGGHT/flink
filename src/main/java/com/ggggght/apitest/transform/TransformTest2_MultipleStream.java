package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import java.util.Collections;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

//@SuppressWarnings("all")
public class TransformTest2_MultipleStream {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStreamSource<String> source =
        env.readTextFile("D:\\source\\flink\\src\\main\\resources\\sensor.txt");

    final SplitStream<SensorReading> splitStream =
        source.map((MapFunction<String, SensorReading>) value -> {
              final String[] split = value.split(",");
              return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            })
            .split((OutputSelector<SensorReading>) value -> value.getTemperature() > 35
                ? Collections.singletonList("high")
                : Collections.singletonList("low"));

    final DataStream<SensorReading> high = splitStream.select("high");
    final DataStream<SensorReading> low = splitStream.select("low");
    final DataStream<SensorReading> all = splitStream.select("high", "low");

    final DataStream<Tuple2<String, Double>> highTuple =
        high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
          @Override public Tuple2<String, Double> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getTemperature());
          }
        });

    final ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = highTuple.connect(low);
    connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
      @Override public Object map1(Tuple2<String, Double> value) throws Exception {
        return new Tuple3<>(value.f0, value.f1, "high temp warning");
      }

      @Override public Object map2(SensorReading value) throws Exception {
        return new Tuple2<>(value.getId(), value.getTemperature());
      }
    }).print();


    env.execute();
  }
}
