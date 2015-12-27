package org.myorg.quickstart;

import java.sql.Timestamp;
import java.util.Map;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.flink.api.common.functions.MapFunction;
import org.myorg.quickstart.calvid.GrokUtils;
import org.myorg.quickstart.calvid.VidPojo;

import com.google.code.regexp.Matcher;

public class CalVidGrokFunction implements MapFunction<String, VidPojo> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private transient Grok grok;
    private String grokPatternPath;
    private String grokExpression;

    private volatile boolean initialized;

    public CalVidGrokFunction(String grokPatternPath, String grokExpression) {
        this.grokPatternPath = grokPatternPath;
        this.grokExpression = grokExpression;
    }

    @Override
    public VidPojo map(String value) throws Exception {

        if (!initialized) {
            init();
        }

        Match m = grok.match(value);

        Map<String, Object> valueMap = null;
        Matcher matcher = m.getMatch();
        if (matcher != null && matcher.matches()) {
            m.captures();

            valueMap = m.toMap();
        }

        VidPojo pojo = new VidPojo();

        //2015-12-24T17:31:00.000-07:00
        
        //17:31:24.13
        if (valueMap != null) {
            String ts = (String) valueMap.get("ts");
            String timestamp = (String) valueMap.get("timestamp");

            int T = timestamp.indexOf('T');
            timestamp = timestamp.substring(0, T + 1) + ts + timestamp.substring(T + 1 + ts.length());

            valueMap.put("timestamp", timestamp);

            BeanUtils.populate(pojo, valueMap);
        }
        return pojo;
    }

    private synchronized void init() {
        try {
            this.grok = GrokUtils.create(grokPatternPath, grokExpression);

            SqlTimestampConverter converter = new SqlTimestampConverter();
            // 2015-12-24T17:31:00.000-07:00
            converter.setPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

            ConvertUtils.register(converter, Timestamp.class);

        } catch (GrokException e) {
            throw new RuntimeException(e);
        } finally {
            initialized = true;
        }
    }
}
