package testworkload;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.List;

public final class CounterMap implements FlatMapFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
    private static final long serialVersionUID = 1L;
    private double totalLatency;
    private double totalDelay;
    private long totalVisit;
    private final long serviceTime; // in millisecond
    public CounterMap(int serviceRate) {
        this.serviceTime = 1/serviceRate * 1000;
//        displayHeader();
    }

    public void displayHeader() {
        System.out.println("Avg Latency, Avg Delay, Total Visits, Total Latency, Total Delay");
    }

    public void displayInfo() {
        System.out.printf("%.4f, %.4f, %d, %.4f, %.4f\n",
                totalLatency/totalVisit,
                totalDelay/totalVisit,
                totalVisit,
                totalLatency,
                totalDelay
                );
    }


    @Override
    public void flatMap(Tuple3<Integer, Long, String> tuple2, Collector<Tuple3<Integer, Long, String>> collector) throws Exception {

        totalDelay += (System.currentTimeMillis()-tuple2.f1)/1000.0;
        Thread.sleep(this.serviceTime);
        totalVisit++;
        double latency = (System.currentTimeMillis()-tuple2.f1)/1000.0;
        totalLatency += latency;

//        displayInfo();
        collector.collect(tuple2);

    }

    public static LinkedHashMap<String, Integer> getTaskDeployRequirement(List<String> allMachine) {
        LinkedHashMap<String, Integer> machineSpec = new LinkedHashMap<>();
//        switch (scenario) {
//            case 1:
//            case 2:
//            case 3:
                machineSpec.put(allMachine.get(0), 2);
                machineSpec.put(allMachine.get(1), 2);
//                break;
//            case 4:
//                machineSpec.put(allMachine.get(0), 4);
//                break;
//            default:
//                break;
//        }
        return machineSpec;
    }

}
