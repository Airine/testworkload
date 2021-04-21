package testworkload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.security.Timestamp;

import static testworkload.utils.StringGenerator.generateString;

public class LargeWordsGenerator implements SourceFunction<Tuple3<Integer, Long, String>> {

    private int count = 0;
    private volatile boolean isRunning = true;

    private final int nKeys;
    private final int rate;       // how many records per second
    private final int nTuples;

    private final String prefix;

    public LargeWordsGenerator(int runtime, int nKeys, int rate, int wordSize) {
        this.nKeys = nKeys;
        this.rate = rate;
        // in byte
        this.nTuples = runtime * rate;

        prefix = generateString(wordSize);

        System.out.println("runtime: " + runtime
                + " nKeys: " + nKeys
                + " rate: " + rate
                + " wordSize: " + wordSize
        );
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, Long, String>> ctx) throws Exception {
        while (isRunning && (count < nTuples)) {
            if (count % rate == 0) {
                Thread.sleep(1000);
            }
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Tuple3<>(count % nKeys, System.currentTimeMillis(),prefix));
                count++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
