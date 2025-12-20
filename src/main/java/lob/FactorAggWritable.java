package lob;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FactorAggWritable implements Writable {

    public static final int DIM = 20;

    public double[] sum = new double[DIM];
    public long count = 0;

    public FactorAggWritable() {}

    // 原有的方法，保留以防万一
    public void add(double[] f) {
        for (int i = 0; i < DIM; i++) sum[i] += f[i];
        count++;
    }

    public void merge(FactorAggWritable other) {
        for (int i = 0; i < DIM; i++) sum[i] += other.sum[i];
        count += other.count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < DIM; i++) out.writeDouble(sum[i]);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < DIM; i++) sum[i] = in.readDouble();
        count = in.readLong();
    }
}