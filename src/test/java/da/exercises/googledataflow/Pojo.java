package da.exercises.googledataflow;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

public final class Pojo implements Serializable {
    private String messageId;
    private Date timestamp;
    private String sender;
    private String subject;
    private String body;
    private String replyTo;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @XmlElement(name = "timestamp")
    @XmlJavaTypeAdapter(DateAdapter.class)
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public String toString() {
        return messageId;
    }

    public static class DateAdapter extends XmlAdapter<String, Date> {

        private SimpleDateFormat fdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");

        @Override
        public synchronized String marshal(Date d) throws Exception {
            return fdf.format(d);
        }

        @Override
        public synchronized Date unmarshal(String s) throws Exception {
            return fdf.parse(s);
        }
    }
}
