package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.counter.CustomCounters;
import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;

/**
 * Mapper parses logs and takes IP and bytes from request.
 * Also it parses user agent part of log string to count requests from different browsers.
 *
 * @author Artem_Iushin <yushin.tema@gmail.com>
 */
public class LogProcessMapper extends Mapper<LongWritable, Text, Text, IpStats> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        MessageFormat format = new MessageFormat("{0} - - [{1}] \"{2}\" {3} {4} \"{5}\" \"{6}\"");
        try {
            Object[] logElements = format.parse(String.valueOf(value));
            getBrowserAndIncrement(context, logElements[6].toString());
            Text ip = new Text(logElements[0].toString());
            IpStats stats = new IpStats(1, Integer.parseInt(logElements[4].toString()));
            context.write(ip, stats);
        } catch (ParseException | NumberFormatException e) {
            context.getCounter(CustomCounters.PARSE_FAIL).increment(1);
        }

    }

    private void getBrowserAndIncrement(Context context, String userAgentString) {
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        Browser browser = userAgent.getBrowser();
        context.getCounter(browser).increment(1);
    }
}
