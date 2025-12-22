package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FactorMapper extends Mapper<LongWritable, Text, LongWritable, FactorAggWritable> {

    // 对象池技术
    // 只创建两个 LobRecord 对象，在处理所有数据行时反复重用
    // 避免为每行数据创建新对象，极大地减少了GC压力
    private LobRecord prevRecord = new LobRecord();
    private LobRecord curRecord = new LobRecord();
    private boolean hasPrev = false; // 标记是否已经有了合法的上一笔数据

    // 预分配计算缓冲区，避免 computeAll内部频繁创建数组。计算出20个因子就把这个数组传进去填满，用完擦除（或覆盖），反复使用
    private final double[] calcBuffer = new double[FactorAggWritable.DIM];

    //记录当前正在处理的文件名，用于检测文件边界
    private String curFile = null;

    //Map端内存聚合
    //在HashMap中缓存聚合结果，而不是每计算一行就context.write一次
    //大幅减少溢写磁盘 (Spill) 的次数
    private final HashMap<Long, FactorAggWritable> aggMap = new HashMap<>(200000);

    @Override
    protected void setup(Context ctx) {
        //初始化状态
        hasPrev = false;
        curFile = null;
        aggMap.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx)
            throws IOException, InterruptedException {

        //获取当前处理的文件名
        String file = ctx.getConfiguration().get("mapreduce.map.input.file");
        //如果文件切换了（CombineTextInputFormat 可能会合并多个文件到一个 Map 任务）
        //必须重置hasPrev，因为不同文件的行情是不连续的，不能跨文件计算Diff
        if (curFile == null || !curFile.equals(file)) {
            curFile = file;
            hasPrev = false;
        }

        String line = value.toString();
        //跳过CSV表头
        if (line.startsWith("tradingDay")) return;

        //指针交换Swap
        //每一轮循环，当前的cur就变成了下一轮的prev。
        //通过交换引用而不是深拷贝数据，实现了 O(1) 的状态转移。
        if (hasPrev) {
            LobRecord temp = prevRecord;
            prevRecord = curRecord;
            curRecord = temp;
        }

        //调用自定义解析器，直接解析数据到 curRecord 对象中 (零对象创建)
        boolean success = LocalCSVParser.parse(line, curRecord);
        if (!success) return;

        long time = curRecord.tradeTime;

        // 数据过滤
        // 9:25~9:30之间的数据可以作为下一笔的 prev
        if (time < 93000) {
            hasPrev = true;
            return;
        }
        // 如果 9:30:00是文件的第一行，没有前一笔数据无法计算Diff类因子
        if (!hasPrev) {
            hasPrev = true;
            return;
        }

        //因子计算
        //此时：prevRecord存的是上一笔有效数据，curRecord是当前数据
        //计算结果直接填入 calcBuffer
        FactorCalculator.computeAll(prevRecord, curRecord, calcBuffer);

        // 生成聚合Key：TradingDay + TradeTime
        // 这样同一秒的多笔数据会被聚合在一起
        long sortKey = curRecord.tradingDay * 1000000L + curRecord.tradeTime;

        //内存聚合，使用HashMap agg 缓存结果，在 cleanup 统一输出。
        //查找当前 Key 是否已有记录，不去立即写磁盘，而是去查 HashMap
        FactorAggWritable agg = aggMap.get(sortKey);
        if (agg == null) {
            agg = new FactorAggWritable();
            aggMap.put(sortKey, agg);
        }

        //如果这个时间点已经有数据了，将当前计算结果累加到 Map 中
        agg.add(calcBuffer);
    }

    //当Map任务处理完所有数据后调用
    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        LongWritable outKey = new LongWritable();

        //统一输出HashMap中的所有聚合结果，遍历（sum[20], count）
        for (Map.Entry<Long, FactorAggWritable> e : aggMap.entrySet()) {
            outKey.set(e.getKey());
            ctx.write(outKey, e.getValue());
        }
        // 释放内存
        aggMap.clear();
    }
}