package lob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FactorCombiner extends Reducer<LongWritable, FactorAggWritable, LongWritable, FactorAggWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<FactorAggWritable> values, Context ctx)
            throws IOException, InterruptedException {

        FactorAggWritable out = new FactorAggWritable();

        for (FactorAggWritable v : values) {
            out.merge(v);
        }

        ctx.write(key, out);
    }
}
