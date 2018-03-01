package com.yuhans.bigdata.hadoop.ipinyouparser;

import com.yuhans.bigdata.hadoop.ipinyouparser.counters.CustomCounter;
import com.yuhans.bigdata.hadoop.ipinyouparser.writable.CityOsWritable;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper get path of the file from Distributed Cache, to pass it to CityMap.
 * In map method it filters events by Event Id and Bidding Price, because we are interested only in impression event
 * and bidding price > 250.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class IpInYouMapper extends Mapper<LongWritable, Text, CityOsWritable, LongWritable> {


    private static final String SEPARATOR = "\t";
    private static final String IMPRESSION_EVENT = "1";
    private static final int MIN_PRICE = 250;
    private static final LongWritable ONE = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] logData = value.toString().split(SEPARATOR);
        try {
            if (IMPRESSION_EVENT.equals(logData[2]) && Integer.parseInt(logData[19]) > MIN_PRICE) {
                Integer city = Integer.parseInt(logData[7]);
                short osId = getOperatingSystemId(logData[4]);
                context.write(new CityOsWritable(city, osId), ONE);
            }
        } catch (NumberFormatException e) {
            context.getCounter(CustomCounter.PARSE_FAIL).increment(1);
        }
    }

    private short getOperatingSystemId(String userAgentLog) {
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentLog);
        return userAgent.getOperatingSystem().getId();
    }
}
