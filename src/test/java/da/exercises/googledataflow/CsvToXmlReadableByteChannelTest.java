package da.exercises.googledataflow;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class CsvToXmlReadableByteChannelTest {

    @Test
    public void testCsvToXml() throws Exception {
        
        URL url =Thread.currentThread().getContextClassLoader().getResource("data.txt");

        CSVParser parser = CSVParser.parse(url, Charset.forName(StandardCharsets.UTF_8.name()), CSVFormat.DEFAULT);

        // @formatter:off
        List<User> usersFromCsv = parser.getRecords().stream().map(record -> {
            User user = new User();
            user.setGivenName(record.get(0));
            user.setMiddleInitial(record.get(1).charAt(0));
            user.setLastName(record.get(2));
            user.setEmailAddress(record.get(6));
            return user;
        }).collect(Collectors.toList());
        //@formatter:on

        //@formatter:off
        try (ReadableByteChannel channel = new CsvToXmlReadableByteChannel(Channels.newChannel(url.openStream()))
                              .withFieldDelimiter(",")
                              .withLineDelimiter("\n")
                              .withFields("givenName", "middleInitial", "lastName", "emailAddress")
                              .withIncludeFields("111000100000000000")
                              .withRecordName("user")) {
        //@formatter:on
            
            JAXBContext jaxbContext = JAXBContext.newInstance(Users.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

            List<User> usersFromXml = ((Users) unmarshaller.unmarshal(Channels.newInputStream(channel))).getUsers();
            
            assertEquals(usersFromCsv.size(), usersFromXml.size());
            
            Assert.assertThat(usersFromXml, CoreMatchers.hasItems(usersFromCsv.toArray(new User[usersFromCsv.size()])));
        }
    }

    private static final class User {
        private String givenName;
        private Character middleInitial;
        private String lastName;
        private String emailAddress;

        public String getGivenName() {
            return givenName;
        }

        public void setGivenName(String givenName) {
            this.givenName = givenName;
        }

        @XmlJavaTypeAdapter(CharacterAdapter.class)
        public Character getMiddleInitial() {
            return middleInitial;
        }

        public void setMiddleInitial(Character middleInitial) {
            this.middleInitial = middleInitial;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getEmailAddress() {
            return emailAddress;
        }

        public void setEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((emailAddress == null) ? 0 : emailAddress.hashCode());
            result = prime * result + ((givenName == null) ? 0 : givenName.hashCode());
            result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
            result = prime * result + middleInitial;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            User other = (User) obj;
            if (emailAddress == null) {
                if (other.emailAddress != null)
                    return false;
            } else if (!emailAddress.equals(other.emailAddress))
                return false;
            if (givenName == null) {
                if (other.givenName != null)
                    return false;
            } else if (!givenName.equals(other.givenName))
                return false;
            if (lastName == null) {
                if (other.lastName != null)
                    return false;
            } else if (!lastName.equals(other.lastName))
                return false;
            if (middleInitial != other.middleInitial)
                return false;
            return true;
        }
    }

    @XmlRootElement(name = "users")
    @XmlAccessorType(XmlAccessType.FIELD)
    private static final class Users {

        @XmlElement(name = "user")
        private List<User> users;

        public List<User> getUsers() {
            return users;
        }

        public void setUsers(List<User> users) {
            this.users = users;
        }
    }
    
    public static class CharacterAdapter extends XmlAdapter<String, Character> {

        @Override
        public synchronized String marshal(Character d) throws Exception {
            return new String(new char[] { d });
        }

        @Override
        public synchronized Character unmarshal(String s) throws Exception {
            if (StringUtils.isNotBlank(s)) {
                return s.charAt(0);
            } else {
                return ' ';
            }
        }
    }
}
