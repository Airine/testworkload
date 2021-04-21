package testworkload;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.LinkedHashMap;
import java.util.List;


public class DummySink implements FlatMapFunction<Tuple3<Integer, Long, String>, Double> {

    private final long serviceTime; // in millisecond
    private double totalLatency;
    private long totalVisit;

    public static final OutputTag<String> latency = new OutputTag<String>("latency"){};

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

    public static LinkedHashMap<String, Integer> getTaskDeployRequirement(List<String> allMachine) {
        LinkedHashMap<String, Integer> machineSpec = new LinkedHashMap<>();
//        switch (scenario) {
//            case 1:
//                machineSpec.put(allMachine.get(2), 4);
//                break;
//            case 2:
//                machineSpec.put(allMachine.get(1), 2);
//                machineSpec.put(allMachine.get(2), 2);
//                break;
//            case 3:
//                machineSpec.put(allMachine.get(0), 2);
//                machineSpec.put(allMachine.get(1), 2);
//                break;
//            case 4:
                machineSpec.put(allMachine.get(0), 4);
//                break;
//            default:
//                break;
//        }

        return machineSpec;
    }

    @Override
    public void flatMap(Tuple3<Integer, Long, String> integerLongStringTuple3, Collector<Double> collector) throws Exception {
        Thread.sleep(this.serviceTime);
        totalVisit++;
        double latency = (System.currentTimeMillis()-integerLongStringTuple3.f1)/1000.0;
        totalLatency += latency;
        displayInfo();
        collector.collect(latency);
    }
}
