package lob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FactorDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: FactorDriver <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        Job job = Job.getInstance(conf, "LOB Factor Calculation");

        job.setJarByClass(FactorDriver.class);

        // Mapper / Reducer
        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入与输出路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);   // 删除旧目录（Hadoop 强制要求）
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 启动任务
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
