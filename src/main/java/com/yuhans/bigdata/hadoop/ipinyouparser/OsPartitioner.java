package com.yuhans.bigdata.hadoop.ipinyouparser;

import com.yuhans.bigdata.hadoop.ipinyouparser.writable.CityOsWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Custom partitioner can be used to create partitions based on OS rather than hashcode.
 *
 * @author Artem_Iushin <yushin.tema@gmail.com>
 */
public class OsPartitioner extends Partitioner<CityOsWritable, LongWritable> {
    @Override
    public int getPartition(CityOsWritable cityOsWritable, LongWritable longWritable, int numPartitions) {
        return cityOsWritable.getOsId() % numPartitions;
    }
}
