package com.yuhans.bigdata.hadoop.longestword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Combiner to diminish mapper output.
 * It should drop words that are less then these that already passed further.
 * As input already comes in sorted order it take only longest words from respective mapper and drop all others.
 *
 * @author Artem Iushin <yushin.tema@gmail.com>
 */
public class LongestWordCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
    private int length = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (length == 0) {
            length = key.get();
        }

        if (key.get() == length) {
            List<Text> output = new ArrayList<>();
            for (Text value : values) {
                if (!output.contains(value)) {
                    output.add(new Text(value));
                    context.write(key, value);
                }
            }
        }
    }
}
