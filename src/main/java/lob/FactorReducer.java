package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Locale;

public class FactorReducer extends Reducer<LongWritable, FactorAggWritable, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> mos;

    private long lastDay = -1;

    private static final String HEADER =
            "tradeTime,alpha_1,alpha_2,alpha_3,alpha_4,alpha_5,alpha_6,alpha_7,alpha_8,alpha_9,alpha_10," +
                    "alpha_11,alpha_12,alpha_13,alpha_14,alpha_15,alpha_16,alpha_17,alpha_18,alpha_19,alpha_20";

    @Override
    protected void setup(Context ctx) {
        mos = new MultipleOutputs<>(ctx);
        Locale.setDefault(Locale.US);
        lastDay = -1;
    }

    @Override
    protected void reduce(LongWritable key, Iterable<FactorAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        double[] sum = new double[FactorAggWritable.DIM];
        long count = 0;

        for (FactorAggWritable v : values) {
            for (int i = 0; i < FactorAggWritable.DIM; i++) sum[i] += v.sum[i];
            count += v.count;
        }

        if (count == 0) return;

        long ts = key.get();
        long day = ts / 1000000L;
        long time = ts % 1000000L;

        String dayStr = String.format("%08d", day);
        String fileName = dayStr.substring(4) + ".csv"; // 0102.csv

        // ⭐ 每天第一次写表头（强烈建议 Driver 设 1 个 reducer，避免重复表头）
        if (day != lastDay) {
            mos.write(NullWritable.get(), new Text(HEADER), fileName);
            lastDay = day;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%06d", time));

        for (int i = 0; i < FactorAggWritable.DIM; i++) {
            double avg = sum[i] / (double) count;
            sb.append(',').append(Double.toString(avg));
        }

        mos.write(NullWritable.get(), new Text(sb.toString()), fileName);
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        mos.close();
    }
}
