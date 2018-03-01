package com.yuhans.bigdata.hadoop.ipinyouparser;

import com.yuhans.bigdata.hadoop.ipinyouparser.writable.CityOsWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;

/**
 * Reducer counts total number of events by city, get name of a city and write totalEvents to context.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class IpInYouReducer extends Reducer<CityOsWritable, LongWritable, Text, LongWritable> {

    private CityMap cityMap;
    private LongWritable totalEvents = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length == 1) {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            cityMap = new CityMap(fileSystem, path);
        } else {
            throw new IOException("Problem with Distributed Cache");
        }
        super.setup(context);
    }

    @Override
    protected void reduce(CityOsWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value: values) {
            totalEvents.set(totalEvents.get() + value.get());
        }
        String cityName = cityMap.getCityNameByCityId(key.getCityName());
        context.write(new Text(cityName), totalEvents);
    }
}
