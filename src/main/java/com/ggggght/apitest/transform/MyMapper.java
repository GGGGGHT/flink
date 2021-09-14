package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("all")
public class MyMapper extends RichMapFunction<SensorReading, Tuple2<String,Integer>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MyMapper.class);

  @Override public Tuple2<String, Integer> map(SensorReading value) throws Exception {
    return new Tuple2<>(value.getId(), value.getId().length());
  }

  /**
   * 初始化工作
   * @param parameters
   * @throws Exception
   */
  @Override public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    System.out.println("open...");
  }

  /**
   * 关闭连接
   * @throws Exception
   */
  @Override public void close() throws Exception {
    super.close();
    System.out.println("close");
  }
}
