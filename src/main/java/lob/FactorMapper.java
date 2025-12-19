package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FactorMapper extends Mapper<LongWritable, Text, LongWritable, FactorAggWritable> {

    private LobRecord prev = null;
    private String curFile = null;

    // in-mapper combine: sortKey -> agg
    private final HashMap<Long, FactorAggWritable> aggMap = new HashMap<>(200000);

    @Override
    protected void setup(Context ctx) {
        prev = null;
        curFile = null;
        aggMap.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx)
            throws IOException, InterruptedException {

        // ✅ CombineTextInputFormat 下：同一个 mapper 会读多个文件
        String file = ctx.getConfiguration().get("mapreduce.map.input.file");
        if (curFile == null || !curFile.equals(file)) {
            curFile = file;
            prev = null; // ⭐必须重置，否则不同股票/不同文件会串起来
        }

        String line = value.toString();
        if (line.startsWith("tradingDay")) return;

        LobRecord cur = LocalCSVParser.parse(line);
        if (cur == null) return;

        long time = cur.tradeTime;

        // 9:25 前：只更新 prev
        if (time < 92500) { prev = cur; return; }
        // 9:25~9:30：只更新 prev，不输出
        if (time < 93000) { prev = cur; return; }
        // prev 为空：先补 prev
        if (prev == null) { prev = cur; return; }

        double[] f = FactorCalculator.computeAll(prev, cur);

        long sortKey = cur.tradingDay * 1000000L + cur.tradeTime;

        FactorAggWritable agg = aggMap.get(sortKey);
        if (agg == null) {
            agg = new FactorAggWritable();
            aggMap.put(sortKey, agg);
        }
        agg.add(f);

        prev = cur;
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        LongWritable outKey = new LongWritable();

        for (Map.Entry<Long, FactorAggWritable> e : aggMap.entrySet()) {
            outKey.set(e.getKey());
            ctx.write(outKey, e.getValue());
        }

        aggMap.clear();
    }
}
