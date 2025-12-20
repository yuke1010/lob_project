package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FactorMapper extends Mapper<LongWritable, Text, LongWritable, FactorAggWritable> {

    // ⭐ 对象池：只创建两个记录对象，反复使用
    private LobRecord prevRecord = new LobRecord();
    private LobRecord curRecord = new LobRecord();
    private boolean hasPrev = false; // 标记 prevRecord 是否有效

    // ⭐ 计算缓冲区：避免每次 new double[20]
    private final double[] calcBuffer = new double[FactorAggWritable.DIM];

    private String curFile = null;

    // in-mapper combine
    private final HashMap<Long, FactorAggWritable> aggMap = new HashMap<>(200000);

    @Override
    protected void setup(Context ctx) {
        hasPrev = false;
        curFile = null;
        aggMap.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx)
            throws IOException, InterruptedException {

        String file = ctx.getConfiguration().get("mapreduce.map.input.file");
        if (curFile == null || !curFile.equals(file)) {
            curFile = file;
            hasPrev = false; // 文件切换，重置前一笔记录
        }

        String line = value.toString();
        if (line.startsWith("tradingDay")) return;

        // ⭐ 1. 交换引用：让 cur 变成 prev (准备接收新数据)
        // 这一步之后，curRecord 指向的是一个"旧对象"，我们可以安全地往里覆写新数据
        // 而 prevRecord 指向的是上一轮解析好的数据
        if (hasPrev) {
            LobRecord temp = prevRecord;
            prevRecord = curRecord;
            curRecord = temp;
        }

        // ⭐ 2. 零拷贝解析：直接解析到 curRecord 中
        boolean success = LocalCSVParser.parse(line, curRecord);
        if (!success) return;

        long time = curRecord.tradeTime;

        // 9:25 前：只更新状态，不算因子
        if (time < 92500) {
            hasPrev = true; // 标记当前这一笔有效，下一轮它就是 prev
            return;
        }
        // 9:25~9:30：同上
        if (time < 93000) {
            hasPrev = true;
            return;
        }
        // 如果没有前一笔数据 (比如文件第一行就是 9:30:00)，没法算 diff，只能先存着
        if (!hasPrev) {
            hasPrev = true;
            return;
        }

        // ⭐ 3. 零 GC 计算：传入 buffer
        // 注意：此时 prevRecord 是上一轮的 cur，curRecord 是刚刚解析的
        FactorCalculator.computeAll(prevRecord, curRecord, calcBuffer);

        long sortKey = curRecord.tradingDay * 1000000L + curRecord.tradeTime;

        FactorAggWritable agg = aggMap.get(sortKey);
        if (agg == null) {
            agg = new FactorAggWritable();
            aggMap.put(sortKey, agg);
        }

        // agg.add 现在需要支持数组参数
        agg.add(calcBuffer);

        // 注意：不需要再赋值 prev = cur，因为我们在开头已经做了 swap
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