package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.Locale;

public class FactorReducer extends Reducer<LongWritable, FactorAggWritable, NullWritable, Text> {

    //用于支持多文件输出 (按日期拆分csv)
    private MultipleOutputs<NullWritable, Text> mos;
    //记录上一行处理的日期，用于判断是否需要写CSV表头
    private long lastDay = -1;

    // 复用 StringBuilder 和 Text 对象以减少 GC 开销
    private final StringBuilder sb = new StringBuilder(512);
    private final Text outValue = new Text();
    private final Text headerText = new Text();

    //CSV输出文件的表头
    private static final String HEADER =
            "tradeTime,alpha_1,alpha_2,alpha_3,alpha_4,alpha_5,alpha_6,alpha_7,alpha_8,alpha_9,alpha_10," +
                    "alpha_11,alpha_12,alpha_13,alpha_14,alpha_15,alpha_16,alpha_17,alpha_18,alpha_19,alpha_20";

    @Override
    protected void setup(Context ctx) {
        mos = new MultipleOutputs<>(ctx);
        //设置Locale防止Double转String时出现逗号
        Locale.setDefault(Locale.US);
        lastDay = -1;
        headerText.set(HEADER);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<FactorAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        double[] sum = new double[FactorAggWritable.DIM];
        long count = 0;

        //全局聚合
        //汇总来自所有 Mapper/Combiner 的结果，把所有mapper中相同时间戳的数据相加
        for (FactorAggWritable v : values) {
            for (int i = 0; i < FactorAggWritable.DIM; i++) sum[i] += v.sum[i];
            count += v.count;
        }

        if (count ==0) return;

        //解析Key：前部分是日期，后部分是时间,如20250101093000
        long ts = key.get();
        long day = ts / 1000000L;//整除运算 (/ 1000000L)：相当于把数字向右移动6位，从而切掉了末尾的时间部分，只保留前面的日期
        long time = ts % 1000000L; //取模运算返回的是除法后的余数。这相当于过滤掉前面的日期部分，只保留末尾的6位时间

        //构造文件名：例如 "0102.csv"
        String dayStr = String.format("%08d", day);
        String fileName = dayStr.substring(4) + ".csv";

        //表头控制逻辑
        //因为只有一个 Reducer 且 Key 在shuffle后已经是有序的，所以数据会按时间顺序流入
        //当日期发生变化时，说明开始处理新的一天，此时写入表头
        if (day != lastDay) {
            mos.write(NullWritable.get(), headerText, fileName);
            lastDay = day;
        }

        //构建 CSV行，缓冲区复用：重置长度
        sb.setLength(0);
        //写入时间列
        sb.append(time);

        //计算全市场20个因子均值并写入
        for (int i = 0; i < FactorAggWritable.DIM; i++) {
            double avg = sum[i] / (double) count;
            sb.append(',').append(avg);
        }

        //使用MultipleOutputs写出到指定文件
        outValue.set(sb.toString());
        mos.write(NullWritable.get(), outValue, fileName);
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        //必须关闭 MultipleOutputs，否则可能丢失数据
        mos.close();
    }
}