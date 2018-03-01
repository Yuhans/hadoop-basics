package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LogProcessMapReduceTest {

    private MapReduceDriver<LongWritable, Text, Text, IpStats, Text, IpStats> mapReduceDriver;

    private String inputTextTemplate = "%s - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 %d \"-\"" +
            " \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private static final String IP1 = "ip1";
    private static final String IP2 = "ip2";

    @Before
    public void setUp() {
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(new LogProcessMapper());
        mapReduceDriver.setCombiner(new LogProcessCombiner());
        mapReduceDriver.setReducer(new LogProcessReducer());
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP1, 4500)));
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP1, 5500)));
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP1, 6500)));
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP1, 7500)));

        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP2, 2000)));
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP2, 1000)));
        mapReduceDriver.withInput(new LongWritable(), new Text(String.format(inputTextTemplate, IP2, 600)));

        mapReduceDriver.withOutput(new Text(IP1), new IpStats(6000, 24000));
        mapReduceDriver.withOutput(new Text(IP2), new IpStats(1200, 3600));

        mapReduceDriver.runTest();
    }
}
