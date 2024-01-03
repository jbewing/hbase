package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ByteBuffBenchmark {
  private static final int BUFFER_SIZE = 4096;

  @Param({"true", "false"})
  public boolean shouldUseNoneRecycler;

  @Param({"true", "false"})
  public boolean useOffHeapBuffer;

  private ByteBuff buffer;

  @Setup
  public void setup() {
    ByteBuffer backingBuffer = useOffHeapBuffer ? ByteBuffer.allocateDirect(BUFFER_SIZE) : ByteBuffer.allocate(BUFFER_SIZE);

    ByteBuffAllocator.Recycler recycler = shouldUseNoneRecycler ? ByteBuffAllocator.NONE : () -> {};

    buffer = new SingleByteBuff(recycler, backingBuffer);

    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer.put((byte) 1);
    }

    buffer.rewind();
  }

  @Benchmark
  public void get(Blackhole blackhole) {
    for (int i = 0; i < BUFFER_SIZE; i++) {
      blackhole.consume(buffer.get());
    }

    buffer.rewind();
  }

  @TearDown
  public void tearDown() {
    buffer.release();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(ByteBuffBenchmark.class.getSimpleName())
      .result("results.txt")
      .resultFormat(ResultFormatType.TEXT)
      .build();
    new Runner(opt).run();
  }
}
