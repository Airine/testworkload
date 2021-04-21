package testworkload;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.List;

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

    public static LinkedHashMap<String, Integer> getTaskDeployRequirement(List<String> allMachine) {
        LinkedHashMap<String, Integer> machineSpec = new LinkedHashMap<>();
        machineSpec.put(allMachine.get(0), 4);
//		machineSpec.put(allMachine.get(1), 2);
        return machineSpec;
    }

}
