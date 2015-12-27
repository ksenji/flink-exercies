package da.flink.exercises.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MailCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = Thread.currentThread().getContextClassLoader().getResource("flinkMails.gz").toExternalForm();
        //@formatter:off
        DataSet<Tuple2<String, String>> mails = env.readCsvFile(filePath)
                                                   .lineDelimiter("##//##")
                                                   .fieldDelimiter("#|#")
                                                   .includeFields(Long.parseLong("000110", 2))
                                                   .types(String.class, String.class);
        
        mails
            .map(new MonthAndEmailMapFunction())
            .groupBy(0, 1)
            .sum(2)
            .print();
        
        //@formatter:on
    }
    
    private static class MonthAndEmailMapFunction implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, String> value) throws Exception {
            String timestamp = value.f0; // 2014-09-10-18:43:16
            String sender = value.f1;

            return Tuple3.of(upToMonth(timestamp), Util.emailOfSender(sender), Integer.valueOf(1));
        }

        private String upToMonth(String timestamp) {
            return timestamp.substring(0, 7);
        }
    }
}
