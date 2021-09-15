package com.ggggght.apitest.transform;

import com.ggggght.bean.SensorReading;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

@SuppressWarnings("all")
public class TransformTest3_Write_MySQL {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStreamSource<String> source =
        env.readTextFile("D:\\source\\flink\\src\\main\\resources\\sensor.txt");

    final DataStream<SensorReading> sensorReadingDataStream = source.map(line -> {
      final String[] split = line.split(",");
      return new SensorReading(split[0], Long.valueOf(split[1]),
          Double.valueOf(split[2]));
    });

    sensorReadingDataStream.addSink(new MySQLSink());
    env.execute();
  }
}

class MySQLSink extends RichSinkFunction<SensorReading> {
  Connection connection = null;
  PreparedStatement insertPs = null;
  PreparedStatement updatePs = null;

  @Override public void open(Configuration parameters) throws Exception {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
    insertPs = connection.prepareStatement("INSERT INTO sensor (id,temp) VALUES (?,?)");
    updatePs = connection.prepareStatement("UPDATE sensor SET temp = ? WHERE id = ?");
  }

  @Override public void invoke(SensorReading value, Context context) throws Exception {
    updatePs.setString(1, value.getId());
    updatePs.setDouble(2, value.getTemperature());
    updatePs.execute();
    if (updatePs.getUpdateCount() == 0) {
      insertPs.setDouble(1, value.getTemperature());
      insertPs.setString(2, value.getId());
    }
  }

  @Override public void close() throws Exception {
    updatePs.close();
    insertPs.close();
    connection.close();
  }
}
