package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer sums up total number of bytes and get average number per request from specific Ip.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class LogProcessReducer extends Reducer<Text, IpStats, Text, IpStats> {

    @Override
    protected void reduce(Text key, Iterable<IpStats> values, Context context) throws IOException, InterruptedException {
        IpStats resultStat = new IpStats();
        for (IpStats stat: values) {
            resultStat.setTotalBytes(resultStat.getTotalBytes() + stat.getTotalBytes());
            resultStat.setAverage(resultStat.getAverage() + stat.getAverage());
        }
        resultStat.setAverage((double) resultStat.getTotalBytes() / resultStat.getAverage());
        context.write(key, resultStat);
    }
}
