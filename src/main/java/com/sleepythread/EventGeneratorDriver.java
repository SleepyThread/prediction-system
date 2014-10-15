package com.sleepythread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EventGeneratorDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();

        configuration.set("startDate", "2014-01-01");
        configuration.set("endDate", "2015-01-01");

        Job job = Job.getInstance(configuration, "Event Generator");

        job.setJarByClass(EventGeneratorDriver.class);
        FileInputFormat.setInputPaths(job, new Path(System.getProperty("inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(System.getProperty("outputPath")));

        job.setMapperClass(EventGeneratorMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new EventGeneratorDriver(), args);
        System.exit(exitCode);
    }
}