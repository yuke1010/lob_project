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
        // 💊 修复 "Filesystem closed" 的特效药
        // 告诉 HDFS/LocalFS：不要缓存连接，每个 Task 独立创建连接
        // -----------------------------------------------------------
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);

        // -----------------------------------------------------------------
        // 🚀 核心修改：动态计算切片大小
        // -----------------------------------------------------------------
        Path inputPath = new Path(args[0]);
        FileSystem fs = inputPath.getFileSystem(conf);
        ContentSummary summary = fs.getContentSummary(inputPath);
        long totalSize = summary.getLength();

        // 目标切片数：5 (适配 4 核 CPU)
        int targetSplits = 5;
        long splitSize = (long) Math.ceil((double) totalSize / targetSplits);


        Job job = Job.getInstance(conf, "LOB-Factor");

        job.setJarByClass(FactorDriver.class);

        job.setMapperClass(FactorMapper.class);
        job.setCombinerClass(FactorCombiner.class);
        job.setReducerClass(FactorReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorAggWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

        // ⭐ 提速：合并小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 🚀 应用刚刚算出来的动态切片大小
        CombineTextInputFormat.setMaxInputSplitSize(job, splitSize);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, inputPath);

        // ⭐ 强制 1 reducer
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}