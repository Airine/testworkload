package testworkload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DummySink implements SinkFunction<Tuple2<String, Integer>> {

    private final long serviceTime; // in millisecond
    public DummySink(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
        Thread.sleep(this.serviceTime);
        System.out.println("value = " + value);
    }
}
