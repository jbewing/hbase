package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.io.WritableUtils;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.*;
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
public class ReadVLongBenchmark {

  @Param({"9", "512", "80000", "548755813887", "1700104028981", "9123372036854775807"})
  public long vlong;

  @Param({"true", "false"})
  public boolean shouldUseNoneRecycler;

  private ByteBuffer ON_HEAP;
  private ByteBuffer ON_HEAP_PADDED;
  private ByteBuffer OFF_HEAP;
  private ByteBuffer OFF_HEAP_PADDED;
  private ByteBuff ON_HEAP_BB;
  private ByteBuff ON_HEAP_PADDED_BB;
  private ByteBuff OFF_HEAP_BB;
  private ByteBuff OFF_HEAP_PADDED_BB;

  @Setup
  public void setup() {
    ON_HEAP = ByteBuffer.allocate(9);
    ON_HEAP_PADDED = ByteBuffer.allocate(9);
    OFF_HEAP = ByteBuffer.allocateDirect(9);
    OFF_HEAP_PADDED = ByteBuffer.allocateDirect(9);

    // Can't use the none recycler for this benchmark
    ByteBuffAllocator.Recycler recycler = shouldUseNoneRecycler ? ByteBuffAllocator.NONE : () -> {};
    ON_HEAP_BB = new SingleByteBuff(recycler, ON_HEAP);
    ON_HEAP_PADDED_BB = new SingleByteBuff(recycler, ON_HEAP_PADDED);
    OFF_HEAP_BB = new SingleByteBuff(recycler, OFF_HEAP);
    OFF_HEAP_PADDED_BB = new SingleByteBuff(recycler, OFF_HEAP_PADDED);

    ByteBufferUtils.writeVLong(ON_HEAP, vlong);
    ON_HEAP.flip();
    ON_HEAP.mark();

    ByteBufferUtils.writeVLong(ON_HEAP_PADDED, vlong);
    for (int i = ON_HEAP_PADDED.position(); i < ON_HEAP_PADDED.capacity(); i++) {
      ON_HEAP_PADDED.put((byte) 0);
    }
    ON_HEAP_PADDED.flip();
    ON_HEAP_PADDED.mark();


    ByteBufferUtils.writeVLong(OFF_HEAP, vlong);
    OFF_HEAP.flip();
    OFF_HEAP.mark();

    ByteBufferUtils.writeVLong(OFF_HEAP_PADDED, vlong);
    for (int i = OFF_HEAP_PADDED.position(); i < OFF_HEAP_PADDED.capacity(); i++) {
      OFF_HEAP_PADDED.put((byte) 0);
    }
    OFF_HEAP_PADDED.flip();
    OFF_HEAP_PADDED.mark();
  }

  @Benchmark
  public void readVLongHBase14186_OnHeapBB(Blackhole blackhole) {
    blackhole.consume(readVLongHBase14186(ON_HEAP_BB));

    ON_HEAP.reset();
  }

  @Benchmark
  public void readVLongHBase14186_OffHeapBB(Blackhole blackhole) {
    blackhole.consume(readVLongHBase14186(OFF_HEAP_BB));

    OFF_HEAP.reset();
  }

  @TearDown
  public void tearDown() {
    ON_HEAP_PADDED_BB.release();
    OFF_HEAP_PADDED_BB.release();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(ReadVLongBenchmark.class.getSimpleName())
      .result("results.txt")
      .resultFormat(ResultFormatType.TEXT)
      .build();
    new Runner(opt).run();
  }

  public static long readVLongHBase14186(ByteBuff buf) {
    byte firstByte = buf.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    } else {
      int remaining = len - 1;
      long i = 0;
      int offsetFromPos = 0;
      if (remaining >= Bytes.SIZEOF_INT) {
        // The int read has to be converted to unsigned long so the & op
        i = (buf.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
        remaining -= Bytes.SIZEOF_INT;
        offsetFromPos += Bytes.SIZEOF_INT;
      }
      if (remaining >= Bytes.SIZEOF_SHORT) {
        short s = buf.getShortAfterPosition(offsetFromPos);
        i = i << 16;
        i = i | (s & 0xFFFF);
        remaining -= Bytes.SIZEOF_SHORT;
        offsetFromPos += Bytes.SIZEOF_SHORT;
      }
      for (int idx = 0; idx < remaining; idx++) {
        byte b = buf.getByteAfterPosition(offsetFromPos + idx);
        i = i << 8;
        i = i | (b & 0xFF);
      }
      buf.skip(len - 1);
      return WritableUtils.isNegativeVInt(firstByte) ? ~i : i;
    }
  }
}
