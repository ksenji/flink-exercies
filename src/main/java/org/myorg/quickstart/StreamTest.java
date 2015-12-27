package org.myorg.quickstart;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableTimestamps();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        List<Tuple2<Long, Integer>> tuples = new ArrayList<>();
        long start = 0;
        for (int i = 1; i < 11; i++) {
            tuples.add(Tuple2.of(start + i * 1000, i));
        }

        env.fromCollection(new SlowIterator<>(tuples, 1000), TypeExtractor.getForObject(Tuple2.of(0L, 0)))
                .assignTimestamps(new AscendingTimestampExtractor<Tuple2<Long, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, Integer> element, long currentTimestamp) {
                        return element.f0;
                    }
                }).timeWindowAll(Time.of(1, TimeUnit.SECONDS)).sum(1).print();

        env.execute("ft");
    }

    private static class SlowIterator<E> implements Iterator<E>, Serializable {

        long sleepFor;
        Collection<E> col;

        transient Iterator<E> delegate;
        volatile boolean initialized;

        public SlowIterator(Collection<E> col, long sleepFor) {
            this.col = col;
            // this.delegate = col.iterator();
            this.sleepFor = sleepFor;
        }

        @Override
        public boolean hasNext() {
            init();
            return delegate.hasNext();
        }

        @Override
        public E next() {
            init();
            try {
                System.out.println("Waiting");
                Thread.sleep(sleepFor);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return delegate.next();
        }

        @Override
        public void remove() {
            init();
            delegate.remove();
        }

        private synchronized void init() {
            if (!initialized) {
                this.delegate = col.iterator();
                initialized = true;
            }
        }
    }
}
