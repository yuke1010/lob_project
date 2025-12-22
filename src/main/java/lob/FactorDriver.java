package lob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FactorDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // -----------------------------------------------------------
        // 关键配置：解决 "Filesystem closed" 异常
        // 在多线程或频繁操作文件系统时，Hadoop 默认会复用 FS 实例。
        // 设置为 true 强制每次获取新的 FS 实例，避免连接被意外关闭
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);

        // -----------------------------------------------------------------
        // 核心优化：基于数据量动态计算切片大小
        // -----------------------------------------------------------------
        Path inputPath = new Path(args[0]);
        FileSystem fs = inputPath.getFileSystem(conf);
        //获取输入目录下所有文件的汇总信息（包括总大小）
        ContentSummary summary = fs.getContentSummary(inputPath);
        long totalSize = summary.getLength();

        // 设定目标并行度：这里设为5，
        // 我们通过优化将IO密集转化为CPU密集任务，为了适配单机环境的4核CPU,按略多于CPU核数设置切片，可以充分利用CPU，避免切片过多导致map任务频繁调度开销过多。
        // 让每个 CPU 核心都有活干，且预留一点余量，经反复试验，在不同数据量上切片数5-6效果最好
        int targetSplits = 5;
        // 计算每个切片的理想字节数，向上取整确保覆盖所有数据
        long splitSize = (long) Math.ceil((double) totalSize / targetSplits);

        //熔断阈值：设阈值为1024MB
        long maxSafeSize = 1024 * 1024 * 1024L;
        // 最终决策，如果数据量大，强制切成更多份，避免内存爆炸。如果数据量小，就按照5个切片切分
        long finalSplitSize = Math.min(splitSize, maxSafeSize);

        // 创建Job实例
        Job job = Job.getInstance(conf, "LOB-Factor");

        job.setJarByClass(FactorDriver.class);

        //设置 Map-Reduce 链条上的两个核心类mapper和Reducer
        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);

        //设置Mapper输出的Key-Value类型(LongWritable 和 FactorAggWritable)
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorAggWritable.class);

        //设置最终输出的 Key-Value类型(Key为空，Value为CSV行)
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

        // -----------------------------------------------------------------
        //输入格式优化：CombineTextInputFormat
        // -----------------------------------------------------------------
        //默认的TextInputFormat会为每个小文件启动一个 Map 任务，这里存在大量小文件，效率极低。
        //CombineTextInputFormat 可以将多个小文件逻辑合并为一个切片。
        job.setInputFormatClass(CombineTextInputFormat.class);

        //将之前计算好的 splitSize 应用到配置中
        //这决定了 Map 任务的数量，直接影响并发度和内存消耗
        CombineTextInputFormat.setMaxInputSplitSize(job, finalSplitSize);

        //递归扫描输入目录（防止数据在子文件夹中）
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, inputPath);

        //强制设置Reducer数量为1
        //目的，确保全局排序方便生成时间序列；方便控制 CSV 表头的输出（只写一次）
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交作业并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}