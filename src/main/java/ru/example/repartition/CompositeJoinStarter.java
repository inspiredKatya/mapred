package ru.example.repartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompositeJoinStarter {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: CompositeJoinDriver <customers> <orders> <products> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Composite Join Example");

        job.setJarByClass(CompositeJoinStarter.class);

        // Set multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CompositeJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CompositeJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CompositeJoinMapper.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TaggedValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(CompositeJoinReducer.class);
        job.setPartitionerClass(CompositeJoinPartitioner.class);

        // Set grouping comparator to group only by join key
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);

        job.setNumReduceTasks(3); // Adjust based on data size

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}