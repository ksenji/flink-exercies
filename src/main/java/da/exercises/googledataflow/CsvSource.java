package da.exercises.googledataflow;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.channels.ReadableByteChannel;
import java.util.BitSet;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.io.XmlSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public class CsvSource<T extends Serializable> extends FileBasedSource<T> {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_MIN_BUNDLE_SIZE = 8 * 1024;

    private CsvSource(String fileName, long minBundleSize, long startOffset, long endOffset) {
        super(fileName, minBundleSize, startOffset, endOffset);
    }

    private CsvSource(String fileOrPatternSpec, long minBundleSize) {
        super(fileOrPatternSpec, minBundleSize);
    }

    private String lineDelimiter = System.lineSeparator();
    private String fieldDelimiter = ",";
    private Class<T> pojoType;
    private String[] fields;
    private String includeFields;

    public static <T extends Serializable> CsvSource<T> from(String fileOrPatternSpec) {
        return new CsvSource<>(fileOrPatternSpec, DEFAULT_MIN_BUNDLE_SIZE);
    }

    public CsvSource<T> withLineDelimiter(String lineDelimiter) {
        CsvSource<T> source = new CsvSource<>(getFileOrPatternSpec(), getMinBundleSize());
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    public CsvSource<T> withFieldDelimiter(String fieldDelimiter) {
        CsvSource<T> source = new CsvSource<>(getFileOrPatternSpec(), getMinBundleSize());
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    public CsvSource<T> withPojoType(Class<T> pojoType) {
        CsvSource<T> source = new CsvSource<>(getFileOrPatternSpec(), getMinBundleSize());
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    public CsvSource<T> withFields(String... fields) {
        CsvSource<T> source = new CsvSource<>(getFileOrPatternSpec(), getMinBundleSize());
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    public CsvSource<T> withIncludeFields(String includeFields) {
        CsvSource<T> source = new CsvSource<>(getFileOrPatternSpec(), getMinBundleSize());
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    @Override
    public FileBasedSource<T> createForSubrangeOfFile(String fileName, long start, long end) {
        CsvSource<T> source = new CsvSource<T>(fileName, getMinBundleSize(), start, end);
        source.lineDelimiter = lineDelimiter;
        source.fieldDelimiter = fieldDelimiter;
        source.pojoType = pojoType;
        source.fields = fields;
        source.includeFields = includeFields;
        return source;
    }

    @Override
    public com.google.cloud.dataflow.sdk.io.FileBasedSource.FileBasedReader<T> createSingleFileReader(PipelineOptions options) {
        return new CsvReader<T>(this, options);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return false;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
        return coder(this);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkArgument(StringUtils.isNotBlank(lineDelimiter), "Line delimiter cannot be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(fieldDelimiter), "Field delimiter cannot be blank");
        Preconditions.checkNotNull(pojoType, "Pojo type cannot be null.");
        Preconditions.checkNotNull(fields, "fields cannot be null");
        Preconditions.checkElementIndex(0, fields.length, "Fields cannot be empty");
        includeFields = StringUtils.trim(includeFields);
        if (StringUtils.isNotBlank(includeFields)) {
            BitSet bs = Util.toBitSet(includeFields);
            Preconditions.checkArgument(bs.cardinality() == fields.length, "Number of includeFields should match the number of fields");
        }
    }

    private <T extends Serializable> Coder<T> coder(CsvSource<T> source) {
        return SerializableCoder.of(source.pojoType);
    }

    protected static class CsvReader<E extends Serializable> extends FileBasedReader<E> {

        private static final Object[] EMPTY_OBJECT = new Object[0];
        private static final Class<?>[] EMPTY_CLASS = new Class<?>[0];

        private final PipelineOptions options;
        private final CsvSource<E> csvSource;
        private final FileBasedSource<E> xmlSource;
        private FileBasedReader<E> reader;
        private CsvToXmlReadableByteChannel delegatedChannel;
        private Method readNextRecordMethod;
        private boolean successfullyDelegated;

        public CsvReader(CsvSource<E> source, PipelineOptions options) {
            super(source);
            this.options = options;
            this.csvSource = source;
            //@formatter:off
            this.xmlSource = XmlSource.<E> from(source.getFileOrPatternSpec())
                                      .withMinBundleSize(source.getMinBundleSize())
                                      .withRecordClass(recordClass(source))
                                      .withRecordElement("row")
                                      .withRootElement("rows")
                                      .createForSubrangeOfFile(source.getFileOrPatternSpec(), source.getStartOffset(), source.getEndOffset());
            // @formatter:on
        }

        private Class<E> recordClass(CsvSource<E> source) {
            return source.pojoType;
        }

        private Method delegateReaderMethod(String methodName, Class<?>... klassArg) {
            Method method = null;
            try {
                method = reader.getClass().getDeclaredMethod(methodName, klassArg);
                method.setAccessible(true);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new RuntimeException(e);
            }
            return method;
        }

        @Override
        protected void startReading(ReadableByteChannel channel) throws IOException {
            reader = xmlSource.createSingleFileReader(options);
            try {
                Method method = delegateReaderMethod("startReading", ReadableByteChannel.class);
                if (method != null) {
                    //@formatter:off
                    delegatedChannel = new CsvToXmlReadableByteChannel(channel)
                                           .withFieldDelimiter(csvSource.fieldDelimiter)
                                           .withLineDelimiter(csvSource.lineDelimiter)
                                           .withFields(csvSource.fields)
                                           .withRecordName("row")
                                           .withIncludeFields(csvSource.includeFields);
                    //@formatter:on
                    method.invoke(reader, delegatedChannel);
                    successfullyDelegated = true;
                }

                readNextRecordMethod = delegateReaderMethod("readNextRecord", EMPTY_CLASS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected boolean readNextRecord() throws IOException {
            boolean read = false;
            if (successfullyDelegated && (readNextRecordMethod != null)) {
                try {
                    read = (Boolean) readNextRecordMethod.invoke(reader, EMPTY_OBJECT);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return read;
        }

        @Override
        protected long getCurrentOffset() throws NoSuchElementException {
            return delegatedChannel.getCurrentOffset();
        }

        @Override
        public E getCurrent() throws NoSuchElementException {
            return reader.getCurrent();
        }
    }

    static final class Util {
        public static BitSet toBitSet(String s) {
            BitSet bs = new BitSet(s.length());

            char[] chars = s.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                char ch = chars[i];
                bs.set(i, ch == '1');
            }
            return bs;
        }

        public static String stringOfAllOnes(int size) {
            // Does not work for more than 63 columns
            if (size < 64) {
                return Long.toBinaryString((1L << size) - 1);
            } else {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < size; i++) {
                    sb.append('1');
                }
                return sb.toString();
            }
        }
    }
}
