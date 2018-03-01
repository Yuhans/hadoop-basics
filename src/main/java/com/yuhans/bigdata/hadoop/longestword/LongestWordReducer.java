package com.yuhans.bigdata.hadoop.longestword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer from a MapReduce job that looks for longest word in series of text inputs.
 * As input here comes in sorted order and Mapper maps in the way that longer words comes first
 * it should take only first word(s).
 *
 * @author Artem Iushin <Artem_Iushin@epam.com>
 */
public class LongestWordReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private int length = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (length == 0) {
            length = key.get();
        }

        if (key.get() == length) {
            List<Text> output = new ArrayList<>();
            for (Text value: values) {
                if (!output.contains(value)) {
                    output.add(new Text(value));
                    context.write(value, new IntWritable(-key.get()));
                }
            }
        }
    }
}
