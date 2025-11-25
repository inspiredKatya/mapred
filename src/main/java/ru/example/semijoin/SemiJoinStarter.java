package ru.example.semijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SemiJoinStarter {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: SemiJoinDriver <orders> <customers> <temp> <output>");
            System.err.println("  <orders>    : Path to recent orders dataset");
            System.err.println("  <customers> : Path to all customers dataset");
            System.err.println("  <temp>      : Temporary path for intermediate results");
            System.err.println("  <output>    : Final output path for active customers");
            System.exit(-1);
        }

        Path ordersPath = new Path(args[0]);
        Path customersPath = new Path(args[1]);
        Path tempPath = new Path(args[2]);
        Path finalOutputPath = new Path(args[3]);

        Configuration conf = new Configuration();

        // Job 1: Extract distinct customer IDs from orders
        System.out.println("Starting Job 1: Extracting distinct customer IDs from orders...");
        boolean job1Success = runDistinctKeysJob(conf, ordersPath, tempPath);
        if (!job1Success) {
            System.err.println("Job 1 failed - cannot proceed with semi-join");
            System.exit(1);
        }
        System.out.println("Job 1 completed successfully!");

        // Job 2: Filter customers using the distinct keys from Job 1
        System.out.println("Starting Job 2: Filtering customers using semi-join...");
        boolean job2Success = runFilterJob(conf, customersPath, tempPath, finalOutputPath);
        if (!job2Success) {
            System.err.println("Job 2 failed");
            System.exit(1);
        }
        System.out.println("Job 2 completed successfully!");

        // Optional: Clean up temporary directory
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true);
                System.out.println("Temporary files cleaned up.");
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not clean up temporary directory: " + e.getMessage());
        }

        System.out.println("Semi-Join completed successfully!");
        System.out.println("Final output available at: " + finalOutputPath);
    }

    /**
     * Job 1: Extract distinct customer IDs who placed recent orders
     */
    private static boolean runDistinctKeysJob(Configuration conf, Path input, Path output)
            throws Exception {

        Job job = Job.getInstance(conf, "Extract Distinct Customer IDs");
        job.setJarByClass(SemiJoinStarter.class);

        // Set mapper and reducer
        job.setMapperClass(OrderKeyMapper.class);
        job.setReducerClass(DistinctKeyReducer.class);

        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set number of reducers for better distribution
        job.setNumReduceTasks(2);

        // Set input and output paths
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Configure to handle CSV with header
        job.setInputFormatClass(TextInputFormat.class);

        return job.waitForCompletion(true);
    }

    /**
     * Job 2: Filter customers dataset using distinct keys from Job 1
     * Uses CustomerFilterMapper which loads active customer IDs in setup method
     */
    private static boolean runFilterJob(Configuration conf, Path customersInput,
                                        Path activeCustomersPath, Path output)
            throws Exception {

        Job job = Job.getInstance(conf, "Filter Customers Using Semi-Join");
        job.setJarByClass(SemiJoinStarter.class);

        // Set mapper and reducer - Using CustomerFilterMapper instead of Cache version
        job.setMapperClass(CustomerFilterMapper.class);
        job.setReducerClass(CustomerFilterReducer.class);

        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use single reducer for final output to get one sorted file
        job.setNumReduceTasks(1);

        // Set input path for customers dataset
        FileInputFormat.addInputPath(job, customersInput);
        FileOutputFormat.setOutputPath(job, output);

        // Configure to handle CSV with header
        job.setInputFormatClass(TextInputFormat.class);

        // Pass the path to active customers data from Job 1 to the mapper
        // The mapper will use this path in setup() to load the active customer IDs
        job.getConfiguration().set("active.customers.dir", activeCustomersPath.toString());

        return job.waitForCompletion(true);
    }

    /**
     * Utility method to print job statistics
     */
    private static void printJobStats(Job job) throws  IOException {
        System.out.println("Job: " + job.getJobName());
        System.out.println("  Map input records: " +
                job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue());
        System.out.println("  Map output records: " +
                job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue());
        System.out.println("  Reduce output records: " +
                job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue());
    }
}
