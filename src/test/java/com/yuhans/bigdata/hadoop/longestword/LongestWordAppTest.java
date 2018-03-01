package com.yuhans.bigdata.hadoop.longestword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordAppTest {
    private static final String WORD1 = "hello";
    private static final String WORD2 = "worlddd";
    private static final String LONG_WORD = "longhello";
    private static final String LONG_WORD2 = "LongWorld";
    private static final Text TEXT = new Text(String.join(" ", WORD1, LONG_WORD, WORD2, LONG_WORD2, LONG_WORD));
    private static final IntWritable W1_WEIGHT = new IntWritable(-5);
    private static final IntWritable W2_WEIGHT = new IntWritable(-7);
    private static final IntWritable LONG_WEIGHT = new IntWritable(-9);

    private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    private ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> mapReduceDriver;

    @Test
    public void testMapper() throws IOException {
        setUpMapper();
        mapDriver.withInput(new LongWritable(), TEXT);
        mapDriver.withOutput(W1_WEIGHT, new Text(WORD1) );
        mapDriver.withOutput(LONG_WEIGHT, new Text(LONG_WORD) );
        mapDriver.withOutput(LONG_WEIGHT, new Text(LONG_WORD2) );
        mapDriver.withOutput(LONG_WEIGHT, new Text(LONG_WORD) );

        mapDriver.runTest(false);
    }

    private void setUpMapper() {
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(new LongestWordMapper());
    }

    @Test
    public void testReducer() throws Exception {
        setUpReducer();

        List<Text> values = new ArrayList<>();
        values.add(new Text(LONG_WORD));
        values.add(new Text(LONG_WORD2));
        values.add(new Text(LONG_WORD));
        reduceDriver.withInput(LONG_WEIGHT, values);

        values = new ArrayList<>();
        values.add(new Text(WORD1));
        reduceDriver.withInput(W1_WEIGHT, values);

        values = new ArrayList<>();
        values.add(new Text(WORD2));
        reduceDriver.withInput(W2_WEIGHT, values);

        reduceDriver.withOutput(new Text(LONG_WORD), new IntWritable(-LONG_WEIGHT.get()));
        reduceDriver.withOutput(new Text(LONG_WORD2), new IntWritable(-LONG_WEIGHT.get()));

        reduceDriver.runTest();
    }

    private void setUpReducer() {
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(new LongestWordReducer());
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(new LongestWordMapper());
        mapReduceDriver.setCombiner(new LongestWordCombiner());
        mapReduceDriver.setReducer(new LongestWordReducer());
        mapReduceDriver.withInput(new LongWritable(), TEXT);
        mapReduceDriver.withOutput(new Text(LONG_WORD), new IntWritable( -LONG_WEIGHT.get()));
        mapReduceDriver.withOutput(new Text(LONG_WORD2), new IntWritable( -LONG_WEIGHT.get()));

        mapReduceDriver.runTest();
    }

}