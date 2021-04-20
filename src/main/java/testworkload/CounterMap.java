package testworkload;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class CounterMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    private final long serviceTime; // in millisecond
    public CounterMap(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
    }

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Thread.sleep(this.serviceTime);
        collector.collect(new Tuple2<String, Integer>(s, 1));
    }
}
