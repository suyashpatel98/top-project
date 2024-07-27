package org.example;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import jdk.incubator.vector.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 3, jvmArgs = {"--add-modules=jdk.incubator.vector"})
public class VectorAPIBenchmark {

    private static final int ARRAY_SIZE = 1024 * 1024;
    private float[] a;
    private float[] b;
    private float[] c;

    @Setup
    public void setup() {
        a = new float[ARRAY_SIZE];
        b = new float[ARRAY_SIZE];
        c = new float[ARRAY_SIZE];
        for (int i = 0; i < ARRAY_SIZE; i++) {
            a[i] = i;
            b[i] = i * 2;
        }
    }

    @Benchmark
    public void scalarAddition(Blackhole bh) {
        for (int i = 0; i < ARRAY_SIZE; i++) {
            c[i] = a[i] + b[i];
        }
        bh.consume(c);
    }

    @Benchmark
    public void vectorAddition(Blackhole bh) {
        VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;
        int upperBound = species.loopBound(ARRAY_SIZE);

        for (int i = 0; i < upperBound; i += species.length()) {
            FloatVector va = FloatVector.fromArray(species, a, i);
            FloatVector vb = FloatVector.fromArray(species, b, i);
            FloatVector vc = va.add(vb);
            vc.intoArray(c, i);
        }

        for (int i = upperBound; i < ARRAY_SIZE; i++) {
            c[i] = a[i] + b[i];
        }
        bh.consume(c);
    }
}
