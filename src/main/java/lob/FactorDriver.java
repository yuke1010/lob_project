package lob;

import org.apache.hadoop.conf.Configuration;
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
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // ⭐ 强制 1 reducer：保证每天只有一个文件 + 表头不重复
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
