package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.counter.CustomCounters;
import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LogProcessMapperTest {
    private MapDriver<LongWritable, Text, Text, IpStats> mapDriver;

    private String inputText = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\"" +
            " \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private String ip = "ip1";
    private int totalBytes = 40028;

    @Before
    public void setUp() {
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(new LogProcessMapper());
    }

    @Test
    public void testMap() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(inputText));
        mapDriver.withOutput(new Text(ip), new IpStats(1,totalBytes));

        mapDriver.runTest();
    }

    @Test
    public void testCustomCounter() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("WrongText"));

        mapDriver.runTest();
        assertEquals(1, mapDriver.getCounters().findCounter(CustomCounters.PARSE_FAIL).getValue());
    }

    @Test
    public void testUserAgent() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(inputText));
        mapDriver.withOutput(new Text(ip), new IpStats(1,totalBytes));

        mapDriver.runTest();
        assertEquals(1, mapDriver.getCounters().findCounter(Browser.BOT).getValue());
    }

}