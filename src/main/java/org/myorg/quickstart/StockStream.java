package org.myorg.quickstart;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StockStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.socketTextStream("localhost", 9999).map(new MapFunction<String, StockPrice>() {
            @Override
            public StockPrice map(String value) throws Exception {
                String[] tokens = value.split(",");
                if (tokens.length == 2) {
                    double price;
                    try {
                        price = Double.parseDouble(tokens[1]);
                    } catch (Exception e) {
                        price = Double.NEGATIVE_INFINITY;
                    }
                    return new StockPrice(tokens[0], price);
                } else {
                    return new StockPrice(null, Double.NEGATIVE_INFINITY);
                }
            }
        });

        DataStream<StockPrice> SPX_STREAM = env.addSource(new ParallelSourceFunction<StockPrice>() {

            private volatile boolean running = true;

            @Override
            public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<StockPrice> ctx) throws Exception {
                int defaultPrice = 1000;
                double price = defaultPrice;
                int sigma = 10;
                Random random = new Random();
                while (running) {
                    price = price + random.nextGaussian() * sigma;
                    ctx.collect(new StockPrice("SPX", price));
                    Thread.sleep(random.nextInt(2000));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        DataStream<StockPrice> union = stream;// .union(SPX_STREAM);
        // union.print();
        // AllWindowedStream<StockPrice, TimeWindow> windowStream = union
        // .timeWindowAll(Time.of(10, TimeUnit.SECONDS),
        // Time.of(5, TimeUnit.SECONDS));

        // windowStream.minBy("price").print();

        // union.keyBy("symbol").timeWindow(Time.of(10, TimeUnit.SECONDS),
        // Time.of(5, TimeUnit.SECONDS)).maxBy("price").print();

        KeyedStream<StockPrice, Tuple> symbolStream = union.keyBy("symbol");

        WindowedStream<StockPrice, Tuple, TimeWindow> windowStream = symbolStream.timeWindow(Time.of(10, TimeUnit.SECONDS),
                Time.of(5, TimeUnit.SECONDS));

        DataStream<StockPrice> avgStream = windowStream.apply(new WindowFunction<StockPrice, StockPrice, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple key, TimeWindow window, Iterable<StockPrice> values, Collector<StockPrice> out) throws Exception {
                String symbol = null;
                double price = 0;
                int count = 0;
                for (StockPrice value : values) {
                    symbol = value.getSymbol();
                    price += value.getPrice().doubleValue();
                    count++;
                }
                // System.out.println(String.format("Symbol: %s, Count= %d",
                // symbol, count));
                out.collect(new StockPrice(symbol, price / count));
            }
        });

        // avgStream.print();

        DataStream<String> priceWarnings = symbolStream.window(GlobalWindows.create()).evictor(DeltaEvictor.of(0.05, new StockPriceDeltaFunction()))
                .trigger(DeltaTrigger.of(0.05, new StockPriceDeltaFunction())).apply(new WindowFunction<StockPrice, String, Tuple, GlobalWindow>() {
                    @Override
                    public void apply(Tuple key, GlobalWindow window, Iterable<StockPrice> values, Collector<String> out) throws Exception {
                        for (StockPrice value : values) {
                            out.collect(value.getSymbol());
                        }
                    }
                });

        // priceWarnings.print();

        priceWarnings.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String symbol) throws Exception {
                return Tuple2.of(symbol, 1);
            }
        }).keyBy("f0").timeWindow(Time.of(30, TimeUnit.SECONDS)).sum("f1").print();

        env.execute("Stock stream");
    }

    private static class StockPriceDeltaFunction implements DeltaFunction<StockPrice> {
        @Override
        public double getDelta(StockPrice oldDataPoint, StockPrice newDataPoint) {
            if (oldDataPoint == newDataPoint) {
                return 0;
            }
            return Math.abs(oldDataPoint.getPrice().doubleValue() - newDataPoint.getPrice().doubleValue() / oldDataPoint.getPrice().doubleValue());
        }
    }
}
