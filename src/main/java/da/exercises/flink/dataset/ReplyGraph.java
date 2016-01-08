package da.exercises.flink.dataset;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReplyGraph {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = Thread.currentThread().getContextClassLoader().getResource("flinkMails.gz").toExternalForm();
        //@formatter:off
        DataSet<Pojo> mails = env.readCsvFile(filePath)
                                 .lineDelimiter("##//##")
                                 .fieldDelimiter("#|#")
                                 .includeFields(Long.parseLong("100101", 2))
                                 .pojoType(Pojo.class, "messageId", "sender", "replyTo");
        
        mails
            .join(mails)
            .where("messageId")
            .equalTo("replyTo")
            .with(new ReplyJoin())
            .groupBy(0, 1)
            .sum(2)
            .print();
        //@formatter:on
    }

    public static final class Pojo {
        private String messageId;
        private String sender;
        private String replyTo;

        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        public String getSender() {
            return sender;
        }

        public void setSender(String sender) {
            this.sender = sender;
        }

        public String getReplyTo() {
            return replyTo;
        }

        public void setReplyTo(String replyTo) {
            this.replyTo = replyTo;
        }
    }

    private static class ReplyJoin implements JoinFunction<Pojo, Pojo, Tuple3<String,String,Integer>> {

        @Override
        public Tuple3<String, String, Integer> join(Pojo first, Pojo second) throws Exception {
            return Tuple3.of(Util.emailOfSender(first.getSender()), Util.emailOfSender(second.getSender()), Integer.valueOf(1));
        }
    }
}
