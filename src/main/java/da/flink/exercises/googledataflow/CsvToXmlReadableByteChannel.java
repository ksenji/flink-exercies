package da.flink.exercises.googledataflow;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;

import org.apache.flink.hadoop.shaded.com.google.common.base.Splitter;
import org.apache.flink.shaded.com.google.common.base.Joiner;
import org.apache.flink.shaded.com.google.common.collect.Iterables;

import da.flink.exercises.googledataflow.CsvSource.Util;

public class CsvToXmlReadableByteChannel implements ReadableByteChannel {

    private static final int SIZE = 8 * 1024;
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    private final ReadableByteChannel delegate;
    private PushbackInputStream stream;
    private ByteArrayOutputStream overall = new ByteArrayOutputStream(SIZE);
    private ByteArrayOutputStream recordStash = new ByteArrayOutputStream(SIZE);
    private ByteArrayOutputStream spilloverStash = new ByteArrayOutputStream(SIZE);

    private byte[] fieldDelimiter;
    private byte[] lineDelimiter;
    private String[] fields;
    private BitSet includeFields;
    private String recordName = "row";

    private boolean foundRecordBoundary = false;
    private boolean available = true;
    private int includedFieldIdx = 0;
    private int fieldIdx = 0;
    private int recordDelimIdx = 0;
    private int fieldDelimIdx = 0;
    private long currentOffset;
    private boolean epilogWithRootWritten;

    public CsvToXmlReadableByteChannel(ReadableByteChannel delegate) {
        this.delegate = delegate;
        this.stream = new PushbackInputStream(Channels.newInputStream(delegate), SIZE);
    }

    private void reset() {
        recordStash.reset();
        spilloverStash.reset();

        foundRecordBoundary = false;
        fieldIdx = 0;
        includedFieldIdx = 0;
        recordDelimIdx = 0;
        fieldDelimIdx = 0;
    }

    private void processField(byte[] buffer, int fieldBoundaryIdx, int prevFieldBoundaryIndex) {
        if (includeFields.get(fieldIdx)) {
            int len = foundRecordBoundary ? lineDelimiter.length : fieldDelimiter.length;
            // <![CDATA[]]
            writeSilently(recordStash, ("<" + fields[includedFieldIdx] + "><![CDATA["));

            int cut = 0;
            if (spilloverStash.size() > 0) {
                if (fieldBoundaryIdx < len) {
                    cut = (len - fieldBoundaryIdx);
                    len -= cut;
                }
                byte[] spilledBytes = spilloverStash.toByteArray();
                recordStash.write(spilledBytes, 0, spilledBytes.length - cut);
            }

            recordStash.write(buffer, prevFieldBoundaryIndex, fieldBoundaryIdx - prevFieldBoundaryIndex - len);
            writeSilently(recordStash, ("]]></" + fields[includedFieldIdx] + ">"));
            includedFieldIdx++;
        }

        spilloverStash.reset();
        fieldDelimIdx = 0;
        fieldIdx++;
    }

    private void processUntilNextRecordBoundary() {
        reset();

        if (!epilogWithRootWritten) {
            writeSilently(overall, "<?xml version=\"1.0\" encoding=\"" + CHARSET + "\"><" + recordName + "s>");
            epilogWithRootWritten = true;
        }

        byte[] buffer = new byte[SIZE];

        int fieldBoundaryIdx = 0;

        while (!foundRecordBoundary && available) {
            int read = -1;
            try {
                read = stream.read(buffer, 0, buffer.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (read != -1) {
                int prevFieldBoundaryIndex = 0;
                for (int i = 0; i < read; i++) {
                    byte b = buffer[i];

                    if (b == fieldDelimiter[fieldDelimIdx]) {
                        fieldDelimIdx++;
                        if (fieldDelimIdx == fieldDelimiter.length) {
                            // found field
                            fieldBoundaryIdx = i + 1;
                            processField(buffer, fieldBoundaryIdx, prevFieldBoundaryIndex);
                            prevFieldBoundaryIndex = fieldBoundaryIdx;
                        }
                    } else {
                        fieldDelimIdx = 0;
                    }

                    if (b == lineDelimiter[recordDelimIdx]) {
                        recordDelimIdx++;
                        if (recordDelimIdx == lineDelimiter.length) {
                            foundRecordBoundary = true;
                            fieldBoundaryIdx = i + 1;
                            currentOffset += fieldBoundaryIdx;
                            processField(buffer, fieldBoundaryIdx, prevFieldBoundaryIndex);
                            try {
                                stream.unread(buffer, fieldBoundaryIdx, read - fieldBoundaryIdx);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            break;
                        }
                    } else {
                        recordDelimIdx = 0;
                    }
                }

                if (!foundRecordBoundary) {
                    currentOffset += read;
                    spilloverStash.write(buffer, fieldBoundaryIdx, read - fieldBoundaryIdx);
                }
            } else {
                available = false;
                if (!foundRecordBoundary && includedFieldIdx > 0) {
                    foundRecordBoundary = true;
                    processField(buffer, fieldBoundaryIdx + lineDelimiter.length, fieldBoundaryIdx); // already
                    // stashed
                }
            }
        }

        if (includedFieldIdx == fields.length) {
            writeSilently(overall, "<" + recordName + ">");
            writeSilently(recordStash, overall);
            writeSilently(overall, "</" + recordName + ">");
        }
    }

    private void writeSilently(ByteArrayOutputStream os, String s) {
        try {
            os.write(s.getBytes(CHARSET));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeSilently(ByteArrayOutputStream from, ByteArrayOutputStream to) {
        try {
            from.writeTo(to);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CsvToXmlReadableByteChannel withFieldDelimiter(String fieldDelimiter) {
        try {
            this.fieldDelimiter = fieldDelimiter.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CsvToXmlReadableByteChannel withIncludeFields(String includeFields) {
        if (includeFields != null) {
            this.includeFields = CsvSource.Util.toBitSet(includeFields);
        }
        return this;
    }

    public CsvToXmlReadableByteChannel withLineDelimiter(String lineDelimiter) {
        try {
            this.lineDelimiter = lineDelimiter.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CsvToXmlReadableByteChannel withRecordName(String name) {
        this.recordName = name;
        return this;
    }

    public CsvToXmlReadableByteChannel withFields(String... fields) {
        this.fields = fields;
        if (this.includeFields == null) {
            withIncludeFields(Util.stringOfAllOnes(fields.length));
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        stream.close();
        delegate.close();
        overall = null;
        recordStash = null;
        spilloverStash = null;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = dst.remaining();

        if (available) {
            while (overall.size() < remaining) {
                if (!available) {
                    writeSilently(overall, "</" + recordName + "s>");
                    break;
                }
                processUntilNextRecordBoundary();
            }
        }

        int writtenSize = -1;

        byte[] buf = overall.toByteArray();
        int len = buf.length;
        if (len > 0) {
            writtenSize = len > remaining ? remaining : len;

            dst.clear();
            dst.put(buf, 0, writtenSize);

            overall.reset();
            if (len > writtenSize) {
                overall.write(buf, writtenSize, len - writtenSize);
            }
        }

        return writtenSize;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public static void main(String[] args) throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource("flinkMails");
        try (InputStream is = url.openStream()) {
            ReadableByteChannel delegate = Channels.newChannel(is);

            //@formatter:off
            try (CsvToXmlReadableByteChannel channel = new CsvToXmlReadableByteChannel(delegate)
                                                           .withFieldDelimiter("#|#")
                                                           .withLineDelimiter("##//##")
                                                           .withFields("f0", "f3", "f5")
                                                           .withIncludeFields("100101")
                                                           .withRecordName("mail")) {
                //@formatter:on

                Iterable<String> paths = Splitter.on('/').split(url.getPath());
                String output = (Joiner.on('/').join(Iterables.limit(paths, Iterables.size(paths) - 1)) + "/flinkMailsOutput").replaceFirst("^/(.:/)",
                        "$1");

                BufferedReader br = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel)));
                Writer writer = Channels.newWriter(Files.newByteChannel(Paths.get(output), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING), StandardCharsets.UTF_8.name());

                String line = null;
                while ((line = br.readLine()) != null) {
                    writer.write(line);
                }
                writer.flush();
                writer.close();

                br.close();
            }
        }
    }

}
