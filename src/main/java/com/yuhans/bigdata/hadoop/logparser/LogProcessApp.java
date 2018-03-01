package com.yuhans.bigdata.hadoop.logparser;

import com.yuhans.bigdata.hadoop.logparser.writableentities.IpStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce Job for processing browser logs and outputs ip, average bytes per request and total bytes by ip.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class LogProcessApp extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new LogProcessApp(), args));
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters need to be supplied - <input dir> and <output dir>");
            return -1;
        }

        Configuration conf = getConf();
        conf.set(TextOutputFormat.SEPERATOR, ",");

        Job job = Job.getInstance(conf, "Process logs");
        job.setJarByClass(LogProcessApp.class);
        job.setMapperClass(LogProcessMapper.class);
        job.setCombinerClass(LogProcessCombiner.class);
        job.setReducerClass(LogProcessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IpStats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpStats.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
