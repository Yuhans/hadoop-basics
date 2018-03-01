package com.yuhans.bigdata.hadoop.ipinyouparser;

import com.yuhans.bigdata.hadoop.ipinyouparser.writable.CityOsWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner summarizes the intermediate result in order to reduce the number of entities transmitted over the network
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class IpInYouCombiner extends Reducer<CityOsWritable, LongWritable, CityOsWritable, LongWritable> {

    private LongWritable result = new LongWritable();

    @Override
    protected void reduce(CityOsWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value: values) {
            result.set(result.get() + value.get());
        }
        context.write(key, result);
    }
}
