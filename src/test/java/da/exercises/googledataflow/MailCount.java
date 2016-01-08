package da.exercises.googledataflow;

import java.net.URL;

import org.apache.commons.lang3.time.FastDateFormat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.CompressedSource;
import com.google.cloud.dataflow.sdk.io.CompressedSource.CompressionMode;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class MailCount {

    public static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        URL file = Thread.currentThread().getContextClassLoader().getResource("flinkMails.gz");

        // @formatter:off
        CsvSource<Pojo> source = CsvSource.<Pojo> from(file.getPath())
                                          .withFieldDelimiter("#|#")
                                          .withLineDelimiter("##//##")
                                          .withPojoType(Pojo.class)
                                          .withFields("timestamp", "sender")
                                          .withIncludeFields("011000");

        CompressedSource<Pojo> compressed = CompressedSource.from(source).withDecompression(CompressionMode.GZIP);
        
        p.apply(Read.from(compressed))
         .apply(new ExtractTimestampAndSender())
         .apply(Count.perElement())
         .apply(MapElements.via(new TimestampAndSenderEmailCompositeKeyFormatter()))
         .apply(TextIO.Write.to("output"));
        //@formatter:on

        p.run();

    }

    private static class ExtractTimestampAndSender extends PTransform<PCollection<Pojo>, PCollection<String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public PCollection<String> apply(PCollection<Pojo> input) {
            return input.apply(ParDo.of(new DoFn<Pojo, String>() {
                private static final long serialVersionUID = 1L;
                private final FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM");

                public String emailOfSender(String sender) {
                    int indexOfLt = sender.indexOf('<') + 1;
                    int indexOfgt = sender.indexOf('>', indexOfLt);
                    return sender.substring(indexOfLt, indexOfgt);
                }

                @Override
                public void processElement(ProcessContext c) throws Exception {
                    Pojo pojo = c.element();

                    c.output(fdf.format(pojo.getTimestamp()) + " " + emailOfSender(pojo.getSender()));
                }
            }));
        }
    }

    private static class TimestampAndSenderEmailCompositeKeyFormatter extends SimpleFunction<KV<String, Long>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + " " + input.getValue();
        }
    }
}
