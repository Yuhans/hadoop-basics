package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LogProcessReducerTest {

    private ReduceDriver<Text, IpStats, Text, IpStats> reduceDriver;

    private Text ip1 = new Text("ip1");
    List<IpStats> ipStats1 = Arrays.asList(new IpStats(1, 3000),
            new IpStats(1, 5000),
            new IpStats(1, 1000));

    private Text ip2 = new Text("ip2");
    List<IpStats> ipStats2 = Arrays.asList(new IpStats(1, 76000),
            new IpStats(1, 24000),
            new IpStats(1, 55000),
            new IpStats(1, 5000));

    @Before
    public void setUp(){
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(new LogProcessReducer());

    }

    @Test
    public void testReduce() throws IOException {
        reduceDriver.withInput(ip1, ipStats1);
        reduceDriver.withInput(ip2, ipStats2);

        reduceDriver.withOutput(ip1, new IpStats(3000,9000));
        reduceDriver.withOutput(ip2, new IpStats(40000, 160000));

        reduceDriver.runTest();
    }
}