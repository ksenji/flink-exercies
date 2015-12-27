package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterationTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Long> someIntegers = env.generateSequence(10, 10);
        
        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
          @Override
          public Long map(Long value) throws Exception {
            return value - 1 ;
          }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
          @Override
          public boolean filter(Long value) throws Exception {
            return (value > 0);
          }
        });

        iteration.closeWith(stillGreaterThanZero);
        
        iteration.print();

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
          @Override
          public boolean filter(Long value) throws Exception {
            return (value <= 0);
          }
        });
        
        lessThanZero.printToErr();
        
        env.execute();
    }
}
