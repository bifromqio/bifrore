package com.bifrore;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class HeapPollJmhBenchmark {
    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    @Param({"1", "64"})
    public int messageCount;

    @Param({"256"})
    public int payloadBytesPerMessage;

    private Object pollBatch;
    private BifroRE.RuleMetadata[] ruleMetadataTable;
    private BifroRE.MessageHandler handler;
    private Method dispatchMethod;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        Class<?> pollBatchClass = Class.forName("com.bifrore.BifroRE$PollBatch");
        Constructor<?> pollBatchCtor = pollBatchClass.getDeclaredConstructor(int[].class, byte[].class);
        pollBatchCtor.setAccessible(true);

        dispatchMethod = BifroRE.class.getDeclaredMethod(
            "dispatchHeapBatch",
            pollBatchClass,
            BifroRE.RuleMetadata[].class,
            BifroRE.MessageHandler.class,
            Executor.class
        );
        dispatchMethod.setAccessible(true);

        ruleMetadataTable = new BifroRE.RuleMetadata[] {
            new BifroRE.RuleMetadata(0, new String[] {"bench"})
        };
        handler = (ruleIndex, payloadBlob, offset, length, metadata) -> {
            if (payloadBlob[offset] == 127) {
                throw new IllegalStateException("unreachable");
            }
        };
        pollBatch = pollBatchCtor.newInstance(buildHeaderTriples(), buildPayloadData());
    }

    @Benchmark
    public void heapDispatchDirectExecutor() throws Exception {
        dispatchMethod.invoke(null, pollBatch, ruleMetadataTable, handler, DIRECT_EXECUTOR);
    }

    private int[] buildHeaderTriples() {
        int[] headerTriples = new int[messageCount * 3];
        for (int i = 0; i < messageCount; i++) {
            int base = i * 3;
            headerTriples[base] = 0;
            headerTriples[base + 1] = i * payloadBytesPerMessage;
            headerTriples[base + 2] = payloadBytesPerMessage;
        }
        return headerTriples;
    }

    private byte[] buildPayloadData() {
        byte[] payloadData = new byte[Math.max(1, messageCount * payloadBytesPerMessage)];
        for (int i = 0; i < payloadData.length; i++) {
            payloadData[i] = (byte) (i & 0x7F);
        }
        return payloadData;
    }
}
