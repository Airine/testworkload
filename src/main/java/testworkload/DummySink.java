package testworkload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.LinkedHashMap;
import java.util.List;

public class DummySink implements SinkFunction<Tuple2<Integer, String>> {

    private final long serviceTime; // in millisecond
    public DummySink(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
    }

    @Override
    public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
        Thread.sleep(this.serviceTime);
        System.out.println("value = " + value);
    }

    public static LinkedHashMap<String, Integer> getTaskDeployRequirement(List<String> allMachine) {
        LinkedHashMap<String, Integer> machineSpec = new LinkedHashMap<>();
        machineSpec.put(allMachine.get(0), 4);
//		machineSpec.put(allMachine.get(1), 2);
        return machineSpec;
    }

}
