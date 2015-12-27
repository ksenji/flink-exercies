package org.myorg.quickstart;

import java.sql.Timestamp;

import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.myorg.quickstart.calvid.VidPojo;

public class CalVidTimestampExtractor implements TimestampExtractor<VidPojo> {

    private static final long serialVersionUID = 1L;

    long currentTimestamp = 1451003400000L;

    @Override
    public long getCurrentWatermark() {
        return currentTimestamp-1;
    }

    @Override
    public long extractWatermark(VidPojo element, long currentTimestamp) {
        long currentTimestamp2 = ts(element, currentTimestamp);
        if (this.currentTimestamp + 2 * 60 * 1000 < currentTimestamp2) {
            System.out.println("Increasing watermark - "+this.currentTimestamp +"->"+currentTimestamp2);
            this.currentTimestamp = currentTimestamp2;
            return this.currentTimestamp;
        } else {
            return Long.MIN_VALUE;
        }

    }

    @Override
    public long extractTimestamp(VidPojo element, long currentTimestamp) {
        if (this.currentTimestamp == -1) {
            this.currentTimestamp = ts(element, currentTimestamp);
        }
        return ts(element, currentTimestamp);
    }

    private long ts(VidPojo element, long currentTimestamp) {
        currentTimestamp = this.currentTimestamp;
        if (element != null) {
            Timestamp ts = element.getTimestamp();
            if (ts != null) {
                currentTimestamp = ts.getTime();
            }
        }
        return currentTimestamp;
    }
}