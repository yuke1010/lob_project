package lob;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FactorAggWritable implements Writable {

    //因子维度，即FactorCalculator计算出的因子数量
    public static final int DIM = 20;

    //存储因子的累加和(用于后续求平均值)
    public double[] sum = new double[DIM];
    //记录聚合的条数
    public long count = 0;

    //必须保留无参构造函数，Hadoop 反序列化时需要反射调用
    public FactorAggWritable() {}

    // 将单次计算结果累加到当前对象 (Map阶段使用)
    public void add(double[] f) {
        for (int i = 0; i < DIM; i++) sum[i]+= f[i];
        count++;
    }

    //将另一个AggWritable对象合并到当前对象
    //实现了来自不同mapper的同一时刻的所有因子的向量加法和计数的累加
    public void merge(FactorAggWritable other) {
        for (int i = 0; i < DIM; i++) sum[i]+= other.sum[i];
        count += other.count;
    }

    //序列化逻辑 (Write)
    //将对象状态写入二进制流，以便网络传输
    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < DIM; i++) out.writeDouble(sum[i]); //20 个 double 数值写进流里
        out.writeLong(count); //把 count 这个长整数写进流里
    }

    //反序列化逻辑 (Read)
    //从二进制流中恢复对象状态
    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < DIM; i++) sum[i] = in.readDouble(); //先读20个double数值
        count = in.readLong(); //再读count这个整数
    }
}