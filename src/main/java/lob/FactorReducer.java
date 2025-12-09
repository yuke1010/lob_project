package lob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FactorReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        final int DIM = 20;
        double[] sum = new double[DIM];
        int count = 0;

        // 累加所有股票的因子值
        for (Text v : values) {
            String[] arr = v.toString().split(",");
            if (arr.length != DIM) continue;   // 保险

            for (int i = 0; i < DIM; i++) {
                sum[i] += Double.parseDouble(arr[i]);
            }
            count++;
        }

        // 如果没有数据，直接跳过
        if (count == 0) return;

        // 求平均
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DIM; i++) {
            if (i > 0) sb.append(',');
            sb.append(sum[i] / count);
        }

        context.write(key, new Text(sb.toString()));
    }
}
