package da.flink.exercises.datastream;

import static com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils.isInNYC;
import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment;

import java.net.URI;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;

public class RideCleansing {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        env.setStreamTimeCharacteristic(EventTime);

        URI filePath = Thread.currentThread().getContextClassLoader().getResource("nycTaxiRides.gz").toURI();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(filePath.getPath(), 60, 600));

        rides.filter(new FilterFunction<TaxiRide>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(TaxiRide ride) throws Exception {
                return isInNYC(ride.startLon, ride.startLat) || isInNYC(ride.endLon, ride.endLat);
            }
        }).print();
        
        env.execute();
    }
}
