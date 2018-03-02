package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner sums up total number of bytes per Ip and counts number of requests.
 * Number of requests is saved in field 'average' in IpStats.
 *
 * @author Artem_Iushin <yushin.tema@gmail.com>
 */
public class LogProcessCombiner extends Reducer<Text, IpStats, Text, IpStats> {
    @Override
    protected void reduce(Text key, Iterable<IpStats> values, Context context) throws IOException, InterruptedException {
        IpStats ipStats = new IpStats();
        for(IpStats stat: values) {
            ipStats.setTotalBytes(ipStats.getTotalBytes() + stat.getTotalBytes());
            ipStats.setAverage(ipStats.getAverage() + stat.getAverage());
        }
        context.write(key, ipStats);
    }
}
