package testworkload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.LinkedHashMap;
import java.util.List;

public class DummySink implements SinkFunction<Tuple3<Integer, Long, String>> {

    private final long serviceTime; // in millisecond
    private double totalLatency;
    private long totalVisit;

    public void displayHeader() {
        System.out.println("Avg Latency, Total Visits, Total Latency");
    }

    public void displayInfo() {
        System.out.printf("%.4f, %d, %.4f\n",
                totalLatency/totalVisit,
                totalVisit,
                totalLatency
        );
    }

    public DummySink(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
        displayHeader();
    }

    @Override
    public void invoke(Tuple3<Integer, Long, String> value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
        Thread.sleep(this.serviceTime);
        totalVisit++;
        totalLatency += (System.currentTimeMillis()-value.f1)/1000.0;
        displayInfo();
    }

    public static LinkedHashMap<String, Integer> getTaskDeployRequirement(List<String> allMachine) {
        LinkedHashMap<String, Integer> machineSpec = new LinkedHashMap<>();
        machineSpec.put(allMachine.get(0), 4);
//		machineSpec.put(allMachine.get(1), 2);
        return machineSpec;
    }

}
