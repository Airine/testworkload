package testworkload;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static testworkload.utils.StringGenerator.generateString;

public class LargeWordsGenerator implements SourceFunction<String> {

    private int runtime;
    private int nKeys;
    private int rate;       // how many records per second
    private int wordSize;   // in byte

    private String prefix;

    public LargeWordsGenerator(int runtime, int nKeys, int rate, int wordSize) {
        this.runtime = runtime;
        this.nKeys = nKeys;
        this.rate = rate;
        this.wordSize = wordSize;

        prefix = generateString(wordSize);

        System.out.println("runtime: " + runtime
                + " nKeys: " + nKeys
                + " rate: " + rate
                + " wordSize: " + wordSize
        );
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
