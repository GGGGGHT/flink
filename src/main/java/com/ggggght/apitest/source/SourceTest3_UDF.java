package com.ggggght.apitest.source;

import com.ggggght.bean.SensorReading;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("all")
public class SourceTest3_UDF {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<SensorReading> dataStream =
        env.addSource(new MySourceFunction());
    dataStream.print();
    env.execute();
  }
}

class MySourceFunction implements SourceFunction<SensorReading> {
  private volatile boolean running = true;

  @Override public void run(SourceContext ctx) throws Exception {
    final Random random = new Random();
    final HashMap<String, Double> map = IntStream.range(0, 10)
        .boxed()
        .collect(Collectors.toMap(i -> "sensor_" + (i + 1), v -> 60 + random.nextGaussian() * 20,
            (a, b) -> b, HashMap::new));

    while (running) {
      for (Map.Entry<String, Double> entry : map.entrySet()) {
        long now = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"));
        map.put(entry.getKey(), entry.getValue() + random.nextGaussian());
        ctx.collect(new SensorReading(entry.getKey(), now, entry.getValue()));
      }

      TimeUnit.SECONDS.sleep(2);
    }
  }

  @Override public void cancel() {
    this.running = false;
  }
}