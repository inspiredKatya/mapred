package ru.example.replicated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ProductClickStarter {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ProductClickStarter <products> <clicks> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Click Starter");

        // Add small products file to Distributed Cache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());

        job.setJarByClass(ProductClickStarter.class);
        job.setMapperClass(ProductClickMapper.class);
        job.setReducerClass(ProductClickReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Set reducer count
        job.setNumReduceTasks(2);

        // Input path for large clicks file
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}