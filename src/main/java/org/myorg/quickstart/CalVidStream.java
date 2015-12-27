package org.myorg.quickstart;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CalVidStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableTimestamps();
        // env.setParallelism(1);

        URL file = Thread.currentThread().getContextClassLoader().getResource("xaa");

        DataStream<String> calvid = env.readTextFile(file.toExternalForm(), "utf-8");

        calvid
        .map(new CalVidGrokFunction("patterns", "%{CALVID}"))
        .assignTimestamps(new CalVidTimestampExtractor())
        .filter(value -> {
            String type = value.getType();
            return (type != null && (type.toUpperCase().contains("ERROR") || type.toUpperCase().contains("WARN")));
        }).map(value -> Tuple3.of(value.getPool(), value.getType(), 1))
        .keyBy("f0", "f1")
        .timeWindow(Time.of(5, TimeUnit.SECONDS))
        .sum("f2")
        .print();

        System.out.println(env.getExecutionPlan());
        
        env.execute("CAL VID");
        
    }
}
