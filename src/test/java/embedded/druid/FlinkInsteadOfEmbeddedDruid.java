package embedded.druid;

import java.lang.reflect.Field;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class FlinkInsteadOfEmbeddedDruid {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = Thread.currentThread().getContextClassLoader().getResource("report.csv").toExternalForm();
        //@formatter:off
        DataSet<Pojo> records = env.readCsvFile(filePath)
                                                   .lineDelimiter("\n")
                                                   .fieldDelimiter(",")
                                                   .includeFields(Long.parseLong("11111101111", 2))
                                                   .pojoType(Pojo.class, "colo", "pool", "report", "url", "metric", "value", "count", "min", "max", "sum");
        
        records.filter(filterFunc("pool", "r1cart"))
               .filter(filterFunc("report", "URLTransaction"))
               .filter(filterFunc("metric", "Duration"))
               .groupBy("url")
               .reduceGroup(new AnalyticsOnGroupFunction())
               .print();
        
        records.filter(filterFunc("pool", "r1cart"))
        .filter(filterFunc("report", "URLTransaction"))
        .filter(filterFunc("metric", "Duration"))
        .groupBy("colo")
        .reduceGroup(new AnalyticsOnGroupFunction())
        .print();
        
        //@formatter:on
    }

    private static FilterFunction<Pojo> filterFunc(final String fieldName, final String valueStr) {
        return new FilterFunction<Pojo>() {

            private transient Field field = lookupField();

            private Field lookupField() {
                Field f = null;
                try {
                    f = Pojo.class.getDeclaredField(fieldName);
                    f.setAccessible(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return f;
            }

            @Override
            public boolean filter(Pojo value) throws Exception {
                if (field == null) {
                    field = lookupField();
                }
                return StringUtils.equals(String.valueOf(field.get(value)), valueStr);
            }
        };
    }

    private static final class AnalyticsOnGroupFunction implements GroupReduceFunction<Pojo, Tuple4<Integer, Integer, Integer, Integer>> {
        @Override
        public void reduce(Iterable<Pojo> values, Collector<Tuple4<Integer, Integer, Integer, Integer>> out) throws Exception {

            final Tuple4<Integer, Integer, Integer, Integer> t4 = Tuple4.of(/* count */ 0, /* min */ Integer.MAX_VALUE, /* max */Integer.MIN_VALUE,
                    /* sum */0);

            values.forEach(new Consumer<Pojo>() {
                @Override
                public void accept(Pojo t) {
                    t4.f0 += t.getCount();

                    if (t.getMin() < t4.f1) {
                        t4.f1 = t.getMin();
                    }

                    if (t.getMax() > t4.f2) {
                        t4.f2 = t.getMax();
                    }

                    t4.f3 += t.getSum();
                }
            });

            out.collect(t4);
        }
    }
}
