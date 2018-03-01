package com.yuhans.bigdata.hadoop.ipinyouparser;

import com.yuhans.bigdata.hadoop.ipinyouparser.writable.CityOsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce job to analyze iPinYou impression, click and conversion log.
 * It counts number of impression by city and spread result between several files
 * based on OS.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class IpInYouApp extends Configured implements Tool {

    private static final String MAPPING_FILE = "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/city.en.txt";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IpInYouApp(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "iPinYou");
        job.setJarByClass(IpInYouApp.class);
        job.addCacheFile(new Path(MAPPING_FILE).toUri());
        job.setPartitionerClass(OsPartitioner.class);
        job.setNumReduceTasks(5);
        job.setMapperClass(IpInYouMapper.class);
        job.setCombinerClass(IpInYouCombiner.class);
        job.setReducerClass(IpInYouReducer.class);
        job.setMapOutputKeyClass(CityOsWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
