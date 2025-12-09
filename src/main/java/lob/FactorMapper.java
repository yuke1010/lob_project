package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper 输入：<offset, line>
// Mapper 输出：key = "20241002,093000", value = "f1,f2,...,f20"
public class FactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String currentStock = null;   // 用于跟踪新文件
    private LobRecord prev = null;        // 前一条记录

    @Override
    protected void setup(Context context) {
        prev = null;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // 跳过 header 行；所有 snapshot.csv 头都以 tradingDay 开头
        if (line.startsWith("tradingDay")) return;

        LobRecord cur = LocalCSVParser.parse(line);
        if (cur == null) return;

        // tradeTime 格式 HHMMSS，不带毫秒（你 LocalTest 已验证过）
        long time = cur.tradeTime;

        // 跳过 09:25 之前的行情
        if (time < 92500) {
            prev = cur;
            return;
        }

        // 09:25～09:30 之间读取 prev，但不输出
        if (time < 93000) {
            prev = cur;
            return;
        }

        // 必须有 prev 才能计算因子
        if (prev == null) {
            prev = cur;
            return;
        }

        // 计算 20 因子
        double[] f = FactorCalculator.computeAll(prev, cur);

        // 组装 value 字符串
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < f.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(f[i]);
        }

        // key = "20241002,093000"
        String mapKey = cur.tradingDay + "," + time;

        context.write(new Text(mapKey), new Text(sb.toString()));

        prev = cur;
    }
}
