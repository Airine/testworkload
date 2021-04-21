package testworkload;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class CounterMap implements FlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {
    private static final long serialVersionUID = 1L;

    private final long serviceTime; // in millisecond
    public CounterMap(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
    }

    @Override
    public void flatMap(Tuple2<Integer, String> tuple2, Collector<Tuple2<Integer, String>> collector) throws Exception {
        Thread.sleep(this.serviceTime);
        collector.collect(tuple2);
    }
}
