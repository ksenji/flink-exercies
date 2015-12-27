package da.flink.exercises.dataset;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MailTFIDF {

    private static final class TfIdfJoiner
            implements JoinFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, Tuple3<String, String, Double>> {
        private static final long serialVersionUID = 1L;

        private final long count;

        public TfIdfJoiner(long count) {
            this.count = count;
        }

        @Override
        public Tuple3<String, String, Double> join(Tuple3<String, String, Integer> tf, Tuple2<String, Integer> df) throws Exception {
            return Tuple3.of(tf.f0, tf.f1, (tf.f2 * count * 1d / df.f1));
        }
    }

    private static final class TermFrequency implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            Iterator<Tuple2<String, String>> iterator = values.iterator();
            Tuple2<String, String> value = iterator.next();
            int count = 1;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            out.collect(Tuple3.of(value.f0, value.f1, count));
        }
    }

    private static final class DocumentFrequency implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
            Iterator<Tuple2<String, String>> iterator = values.iterator();
            Tuple2<String, String> value = iterator.next();
            int count = 1;

            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            
            out.collect(Tuple2.of(value.f1, count));
        }
    }

    private static final class TokenizeFunction implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws Exception {
            String id = value.f0;
            String body = value.f1;

            String[] tokens = StringUtils.split(body);
            for (String token : tokens) {
                if (StringUtils.isAlphanumeric(token)) {
                    String lowerCasedToken = StringUtils.lowerCase(token);
                    if (!StopWordsHolder.STOP_WORDS.contains(lowerCasedToken)) {
                        out.collect(Tuple2.of(id, lowerCasedToken));
                    }
                }
            }
        }
    }

    private static class StopWordsHolder {
        private final static String[] STOP_WORDS_ARR = { "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
                "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you", "have", "as", "can" };

        public final static Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList(STOP_WORDS_ARR));
    }

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = Thread.currentThread().getContextClassLoader().getResource("flinkMails.gz").toExternalForm();
        //@formatter:off
        DataSet<Tuple2<String,String>> mails = env.readCsvFile(filePath)
                                 .lineDelimiter("##//##")
                                 .fieldDelimiter("#|#")
                                 .includeFields(Long.parseLong("010001", 2))
                                 .types(String.class, String.class);
        
        DataSet<Tuple2<String, String>> tokenized = mails.flatMap(new TokenizeFunction());
        DataSet<Tuple3<String, String, Integer>> termFrequency = tokenized.groupBy(0, 1).reduceGroup(new TermFrequency());
        DataSet<Tuple2<String, Integer>> documentFrequency = tokenized.distinct().groupBy(1).reduceGroup(new DocumentFrequency());

        termFrequency
                    .join(documentFrequency)
                    .where(1)
                    .equalTo(0)
                    .with(new TfIdfJoiner(mails.count()))
                    .writeAsText("tfidf"); //writes to tfidf directory

        //@formatter:on
        env.execute();
    }
}
