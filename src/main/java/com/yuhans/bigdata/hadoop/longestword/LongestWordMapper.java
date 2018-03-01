package com.yuhans.bigdata.hadoop.longestword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper that gets words from an input text.
 * This also incorporates preliminary filtering: words that are shorter then that already found are omitted from output.
 * To benefit from natural sorting that is working out of the box in MapReduce words' length is put negated,
 * so longer words will come first.
 *
 * @author Artem Iushin <Artem_Iushin@epam.com>
 */
public class LongestWordMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Text word = new Text();
    private int maxLength = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            if (word.getLength() >= maxLength) {
                maxLength = word.getLength();
                context.write(new IntWritable(-word.getLength()), word);
            }
        }
    }
}
